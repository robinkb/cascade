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
	"testing"
	"time"

	"github.com/robinkb/cascade/controller/core"
	"github.com/stretchr/testify/suite"
)

// ControllerConstructor is a function which returns a new core.Controller
type ControllerConstructor func(clusterName string, clusterRoute *core.ClusterRoute) (core.Controller, error)

// Controller runs the ControllerTestSuite
func Controller(t *testing.T, constructor ControllerConstructor) {
	suite.Run(t, &ControllerTestSuite{
		Constructor: constructor,
	})
}

type ControllerTestSuite struct {
	suite.Suite
	Constructor ControllerConstructor
}

// TestClusterFormation verifies that a set of controllers can form a cluster.
func (suite *ControllerTestSuite) TestClusterFormation() {
	clusterName := "test-cluster-formation"
	clusterRoute := &core.ClusterRoute{
		ServerName: "s1",
		// IPAddr:     "localhost",
		Port: -1,
	}
	c1, err := suite.Constructor(clusterName, clusterRoute)
	suite.Require().NoError(err)

	c1.Start()

	clusterRoute.ServerName = "s2"
	c2, err := suite.Constructor(clusterName, clusterRoute)
	suite.Require().NoError(err)

	c2.Start()

	time.Sleep(10 * time.Second)
}
