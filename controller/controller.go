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
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/robinkb/cascade/controller/nats"
)

func NewController(sd ServiceDiscovery, nats *nats.Server) Controller {
	c := &controller{
		sd:   sd,
		nats: nats,

		quit:             make(chan struct{}),
		shutdownComplete: make(chan struct{}),
		errs:             make(chan error, 1),
	}
	return c
}

type controller struct {
	sd   ServiceDiscovery
	nats *nats.Server

	quit             chan struct{}
	shutdownComplete chan struct{}
	errs             chan error
}

func (c *controller) Start() {
	c.sd.Start(c.quit)
	if err := c.nats.Start(); err != nil {
		// TODO: Don't panic
		panic(err)
	}

	// TODO: Type returned by Server should better align with what we need.
	u := c.nats.ClusterRoute()
	host := strings.Split(u.Host, ":")[0]
	port, _ := strconv.Atoi(strings.Split(u.Host, ":")[1])
	clusterRoute := &ClusterRoute{
		ServerName: c.nats.Name(),
		IPAddr:     host,
		Port:       int32(port),
	}
	c.sd.Register(clusterRoute)

	go func() {
		for {
			routes, err := c.sd.Routes()
			if err != nil {
				// TODO: Don't panic
				panic(err)
			}

			if err := c.nats.Routes(routes); err != nil {
				// TODO: Don't panic
				panic(err)
			}

			select {
			case <-c.quit:
				return
			case <-c.sd.Refresh():
				// continue
			case <-time.After(10 * time.Minute):
				// continue
			}
		}
	}()

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
	// if c.ns != nil && c.ns.Running() {
	// 	c.ns.Shutdown()
	// 	c.ns.WaitForShutdown()
	// }

	close(c.shutdownComplete)
}

// WaitForShutdown will block until the server has been fully shutdown.
func (c *controller) WaitForShutdown() {
	<-c.shutdownComplete
}

// func (c *controller) discoveryManagement() {
// 	id := c.nso.ServerName
// 	u := &url.URL{
// 		Host: fmt.Sprintf("%s:%d", c.nso.Cluster.Host, c.nso.Cluster.Port),
// 	}

// 	go func() {
// 		for {
// 			// Should maybe just call `AddEndpoint` every time,
// 			// in case the URL ever changes?
// 			if !c.dc.Registered(id) {
// 				c.dc.Set(id, u)
// 			}

// 			c.endpoints = c.dc.Routes()

// 			select {
// 			case <-c.quit:
// 				return
// 			case <-c.dc.Refresh():
// 				// continue
// 			case <-time.After(60 * time.Second):
// 				// continue
// 			}
// 		}
// 	}()
// }

// func (c *controller) natsManagement() {
// 	go func() {
// 		for {
// 			time.Sleep(1 * time.Second)

// 			if len(c.endpoints) != 3 {
// 				continue
// 			}

// 			if slices.Equal(c.endpoints, c.nso.Routes) {
// 				continue
// 			}

// 			c.nso.Routes = c.endpoints

// 			if c.ns != nil && c.ns.Running() {
// 				c.ns.Shutdown()
// 				c.ns.WaitForShutdown()
// 			}

// 			ns, err := nats.NewServer(c.nso)
// 			if err != nil {
// 				log.Print(err)
// 				continue
// 			}

// 			ns.ConfigureLogger()

// 			if err := nats.Run(ns); err != nil {
// 				log.Print(err)
// 				continue
// 			}

// 			c.ns = ns

// 			select {
// 			case <-c.quit:
// 				return
// 			case <-time.After(60 * time.Second):
// 				continue
// 			}
// 		}
// 	}()
// }

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

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("ip not found")
}
