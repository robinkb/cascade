package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry"
	nats "github.com/nats-io/nats-server/v2/server"

	_ "github.com/robinkb/cascade-registry/registry/storage/driver"
)

func NewController() *controller {
	c := &controller{}
	c.errs = make(chan error, 1)
	return c
}

type controller struct {
	ns *nats.Server
	rg *registry.Registry

	errs chan error
}

func (c *controller) Start() error {
	if err := c.startNats(); err != nil {
		return err
	}

	if !c.ns.ReadyForConnections(4 * time.Second) {
		return errors.New("nats start timeout")
	}

	if err := c.startRegistry(); err != nil {
		return err
	}

	c.handleSignals()

	return nil
}

func (c *controller) startNats() error {
	opts := &nats.Options{
		JetStream: true,
	}
	ns, err := nats.NewServer(opts)
	ns.ConfigureLogger()
	if err != nil {
		return err
	}
	c.ns = ns

	go ns.Start()
	return nil
}

// I swear that this is the easiest way to do it.
const registryConf = `
version: 0.1
http:
  addr: ":5000"
storage:
  nats: {}
`

func (c *controller) startRegistry() error {
	ctx := context.Background()

	config, err := configuration.Parse(bytes.NewReader([]byte(registryConf)))
	if err != nil {
		return err
	}

	rg, err := registry.NewRegistry(ctx, config)
	if err != nil {
		return err
	}
	c.rg = rg

	go func() {
		err := rg.ListenAndServe()
		if err != nil {
			c.errs <- err
		}
	}()

	return nil
}

func (c *controller) handleSignals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)
	go func() {
		sig := <-sigs
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			if err := c.rg.Shutdown(); err != nil {
				// TODO: Forward this to a proper logger.
				fmt.Printf("failed to gracefully shutdown embedded registry: %s", err)
			}
			c.ns.Shutdown()
			c.ns.WaitForShutdown()
		}
		done <- true
	}()

	<-done
}
