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
package testsuites

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/robinkb/cascade/controller"
	"github.com/stretchr/testify/suite"
)

// ServiceDiscoveryConstructor is a function which returns a new controller.ServiceDiscovery
type ServiceDiscoveryConstructor func(clusterName string) (controller.ServiceDiscovery, error)

// ServiceDiscovery runs the ServiceDiscoveryTestSuite
func ServiceDiscovery(t *testing.T, serviceDiscoveryConstructor ServiceDiscoveryConstructor) {
	suite.Run(t, &ServiceDiscoveryTestSuite{
		Constructor: serviceDiscoveryConstructor,
		ctx:         context.Background(),
	})
}

type ServiceDiscoveryTestSuite struct {
	suite.Suite
	Constructor ServiceDiscoveryConstructor
	ctx         context.Context
}

// TestRegisterSelf verifies that a ServiceDiscovery can register and return itself.
func (suite *ServiceDiscoveryTestSuite) TestRegisterSelf() {
	clusterName := "register1"
	definedRoute := &controller.ClusterRoute{
		ServerName: "s1",
		IPAddr:     "192.168.0.11",
		Port:       randomPort(),
	}

	sd, err := suite.Constructor(clusterName)
	suite.Require().NoError(err)

	sd.Start(suite.ctx.Done())
	sd.Register(definedRoute)

	time.Sleep(500 * time.Millisecond)

	discoveredRoutes, err := sd.Routes()
	suite.Require().NoError(err)

	if len(discoveredRoutes) != 1 {
		suite.T().Fatalf("expected 1 route, got %d", len(discoveredRoutes))
	}

	discoveredRoute := discoveredRoutes[0]
	suite.Require().Equal(clusterRouteToHost(definedRoute), discoveredRoute.Host)
}

// TestRegisterThree verifies that three ServiceDiscoveries can register
// and discover each other's route.
func (suite *ServiceDiscoveryTestSuite) TestRegisterThree() {
	clusterName := "register3"
	definedRoutes := []*controller.ClusterRoute{
		{
			ServerName: "s1",
			IPAddr:     "192.168.0.11",
			Port:       randomPort(),
		},
		{
			ServerName: "s2",
			IPAddr:     "192.168.0.12",
			Port:       randomPort(),
		},
		{
			ServerName: "s3",
			IPAddr:     "192.168.0.13",
			Port:       randomPort(),
		},
	}
	expectedRoutes := len(definedRoutes)

	serviceDiscoveries := make([]controller.ServiceDiscovery, 0)
	for _, route := range definedRoutes {
		serviceDiscovery, err := suite.Constructor(clusterName)
		suite.Require().NoError(err)

		serviceDiscovery.Start(suite.ctx.Done())
		serviceDiscovery.Register(route)

		serviceDiscoveries = append(serviceDiscoveries, serviceDiscovery)
	}

	for _, serviceDiscovery := range serviceDiscoveries {
		var discoveredRoutes []*url.URL
		tries := 0
		for {
			var err error
			discoveredRoutes, err = serviceDiscovery.Routes()
			suite.Require().NoError(err)
			suite.T().Logf("found %d routes", len(discoveredRoutes))

			if len(discoveredRoutes) != expectedRoutes {
				tries++
				if tries == 3 {
					suite.T().Fatalf("discovered %d routes, expected %d", len(discoveredRoutes), expectedRoutes)
				}

				time.Sleep(500 * time.Millisecond)
				continue
			}

			break
		}

		matchedRoutes := 0
		for _, definedRoute := range definedRoutes {
			for _, discoveredRoute := range discoveredRoutes {
				suite.T().Logf("defined: %s, found: %s", clusterRouteToHost(definedRoute), discoveredRoute.Host)
				if clusterRouteToHost(definedRoute) == discoveredRoute.Host {
					matchedRoutes++
				}
			}
		}

		if matchedRoutes != expectedRoutes {
			suite.T().Errorf("service discovery matched %d routes, expected %d", matchedRoutes, expectedRoutes)
		}
	}
}

// TestRegisterThree verifies that ServiceDiscoveries for different clusters
// do not discover each other's routes.
func (suite *ServiceDiscoveryTestSuite) TestDifferentClusters() {
	clusterA := "cluster-a"
	clusterRouteA := &controller.ClusterRoute{
		ServerName: "s1",
		IPAddr:     "192.168.0.11",
		Port:       randomPort(),
	}

	clusterB := "cluster-b"
	clusterRouteB := &controller.ClusterRoute{
		ServerName: "s1",
		IPAddr:     "172.16.0.1",
		Port:       randomPort(),
	}

	serviceDiscoveryA, err := suite.Constructor(clusterA)
	suite.Require().NoError(err)

	serviceDiscoveryA.Start(suite.ctx.Done())
	serviceDiscoveryA.Register(clusterRouteA)

	serviceDiscoveryB, err := suite.Constructor(clusterB)
	suite.Require().NoError(err)

	serviceDiscoveryB.Start(suite.ctx.Done())
	serviceDiscoveryB.Register(clusterRouteB)

	routesA, err := serviceDiscoveryA.Routes()
	suite.Require().NoError(err)
	suite.Require().Len(routesA, 1)
	suite.Require().Equal(clusterRouteToHost(clusterRouteA), routesA[0].Host)

	routesB, err := serviceDiscoveryB.Routes()
	suite.Require().NoError(err)
	suite.Require().Len(routesB, 1)
	suite.Require().Equal(clusterRouteToHost(clusterRouteB), routesB[0].Host)
}

// TestRefresh verifies that discovering routes triggers the Refresh.
func (suite *ServiceDiscoveryTestSuite) TestRefresh() {
	clusterName := "refresh"
	definedRoutes := []*controller.ClusterRoute{
		{
			ServerName: "s1",
			IPAddr:     "192.168.0.11",
			Port:       randomPort(),
		},
		{
			ServerName: "s2",
			IPAddr:     "192.168.0.12",
			Port:       randomPort(),
		},
		{
			ServerName: "s3",
			IPAddr:     "192.168.0.13",
			Port:       randomPort(),
		},
	}

	var wg sync.WaitGroup
	for _, route := range definedRoutes {
		serviceDiscovery, err := suite.Constructor(clusterName)
		suite.Require().NoError(err)

		wg.Add(1)
		go func() {
			select {
			case <-serviceDiscovery.Refresh():
			case <-time.After(2 * time.Second):
				suite.T().Error("refresh not triggered")
			}
			wg.Done()
		}()

		serviceDiscovery.Start(suite.ctx.Done())
		serviceDiscovery.Register(route)
	}

	wg.Wait()
}

func clusterRouteToHost(clusterRoute *controller.ClusterRoute) string {
	return fmt.Sprintf("%s:%d", clusterRoute.IPAddr, clusterRoute.Port)
}

func randomPort() int32 {
	return rand.Int31n(50000) + 1024
}
