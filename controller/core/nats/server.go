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
package nats

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	nats "github.com/nats-io/nats-server/v2/server"
	"golang.org/x/exp/maps"
)

func NewServer(options *nats.Options) *Server {
	return &Server{
		options: options,
		routes:  make(map[string]*url.URL),
	}
}

type Server struct {
	server  *nats.Server
	options *nats.Options
	routes  map[string]*url.URL
}

func (s *Server) Start() error {
	ns, err := nats.NewServer(s.options.Clone())
	if err != nil {
		return err
	}

	s.server = ns
	s.server.SetLoggerV2(logger.NewTestLogger(s.options.ServerName, false), false, false, false)

	s.server.Start()

	if !s.server.ReadyForConnections(10 * time.Second) {
		return nats.ErrServerNotRunning
	}

	return nil
}

func (s *Server) Name() string {
	return s.server.Name()
}

func (s *Server) ClusterRoute() *url.URL {
	clusterAddr := s.server.ClusterAddr()
	ip := clusterAddr.IP
	if ip.String() == "::" {
		ip = getLocalIP()
	}
	return nats.RoutesFromStr(
		fmt.Sprintf("nats://%s:%d", ip.String(), clusterAddr.Port),
	)[0]
}

func (s *Server) ActivePeers() int {
	return len(s.server.ActivePeers())
}

func (s *Server) Routes(routes []*url.URL) error {
	slices.SortFunc(routes, sortRoutes)
	slices.SortFunc(s.options.Routes, sortRoutes)
	if !slices.Equal(routes, s.options.Routes) {
		s.options.JetStream = true
		s.options.Routes = routes
		return s.Reload()
	}

	return nil
}

func (s *Server) SetRoute(id string, u *url.URL) error {
	s.routes[id] = u
	routes := maps.Values(s.routes)
	slices.SortFunc(routes, sortRoutes)
	slices.SortFunc(s.options.Routes, sortRoutes)

	if !slices.Equal(routes, s.options.Routes) {
		s.options.JetStream = true
		s.options.Routes = routes
		return s.Reload()
	}

	return nil
}

func (s *Server) Reload() error {
	if s.server == nil || !s.server.Running() {
		return nats.ErrServerNotRunning
	}

	return s.server.ReloadOptions(s.options.Clone())
}

func sortRoutes(a, b *url.URL) int {
	if a.String() < b.String() {
		return -1
	}

	if a.String() > b.String() {
		return 1
	}

	if a.String() == b.String() {
		return 0
	}

	return 0
}

// getLocalIP returns the non loopback local IP of the host
func getLocalIP() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP
			}
		}
	}
	return nil
}
