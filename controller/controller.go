/*
Copyright © 2024 Robin Ketelbuters

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package controller

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry"
	nats "github.com/nats-io/nats-server/v2/server"

	_ "github.com/robinkb/cascade/registry/storage/driver"
)

func NewController(dc *discoveryClient, natsOptions *nats.Options, registryConfig *configuration.Configuration) *controller {
	c := &controller{
		dc:  dc,
		nso: natsOptions,
		rgc: registryConfig,

		quit:             make(chan struct{}),
		shutdownComplete: make(chan struct{}),
		errs:             make(chan error, 1),
	}
	return c
}

type controller struct {
	dc        *discoveryClient
	endpoints []*url.URL

	ns  *nats.Server
	nso *nats.Options

	rg  *registry.Registry
	rgc *configuration.Configuration

	quit             chan struct{}
	shutdownComplete chan struct{}
	errs             chan error
}

func (c *controller) Run() {
	log.Print("starting discovery management")
	c.discoveryManagement()
	log.Print("starting nats management")
	c.natsManagement()

	// Should wait for NATS to be running before we start the registry.
	// c.rgc.Storage["nats"] = configuration.Parameters{
	// 	"clienturl": c.ns.ClientURL(),
	// }

	log.Print("handling signals")
	c.handleSignals()
}

func (c *controller) Shutdown() {
	// Stops various go routines waiting for this channel.
	close(c.quit)

	// if err := c.rg.Shutdown(context.Background()); err != nil {
	// 	// TODO: Forward this to a proper logger.
	// 	fmt.Printf("failed to gracefully shutdown embedded registry: %s", err)
	// }
	if c.ns != nil && c.ns.Running() {
		c.ns.Shutdown()
		c.ns.WaitForShutdown()
	}

	close(c.shutdownComplete)
}

// WaitForShutdown will block until the server has been fully shutdown.
func (c *controller) WaitForShutdown() {
	<-c.shutdownComplete
}

func (c *controller) discoveryManagement() {
	id := c.nso.ServerName
	u := &url.URL{
		Host: fmt.Sprintf("%s:%d", c.nso.Cluster.Host, c.nso.Cluster.Port),
	}

	go func() {
		for {
			// Should maybe just call `AddEndpoint` every time,
			// in case the URL ever changes?
			if !c.dc.Registered(id) {
				c.dc.SetEndpoint(id, u)
			}

			c.endpoints = c.dc.Endpoints()

			select {
			case <-c.quit:
				return
			case <-c.dc.Refresh():
				// continue
			case <-time.After(60 * time.Second):
				// continue
			}
		}
	}()
}

func (c *controller) natsManagement() {
	go func() {
		for {
			time.Sleep(1 * time.Second)

			if len(c.endpoints) != 3 {
				continue
			}

			if slices.Equal(c.endpoints, c.nso.Routes) {
				continue
			}

			c.nso.Routes = c.endpoints

			if c.ns != nil && c.ns.Running() {
				c.ns.Shutdown()
				c.ns.WaitForShutdown()
			}

			ns, err := nats.NewServer(c.nso)
			if err != nil {
				log.Print(err)
				continue
			}

			ns.ConfigureLogger()

			if err := nats.Run(ns); err != nil {
				log.Print(err)
				continue
			}

			c.ns = ns

			select {
			case <-c.quit:
				return
			case <-time.After(60 * time.Second):
				continue
			}
		}
	}()
}

// func (c *controller) startRegistry() error {
// 	ctx := context.Background()

// 	rg, err := registry.NewRegistry(ctx, c.rgc)
// 	if err != nil {
// 		return err
// 	}
// 	c.rg = rg

// 	go func() {
// 		err := rg.ListenAndServe()
// 		if err != nil {
// 			c.errs <- err
// 		}
// 	}()

// 	return nil
// }

func (c *controller) handleSignals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			c.Shutdown()
		}
	}()
}
