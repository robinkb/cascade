// Copyright 2024 Robin Ketelbuters
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"net"
	"testing"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"

	"github.com/nats-io/nats-server/v2/server"
)

var ns *server.Server

func newDriverConstructor(tb testing.TB) testsuites.DriverConstructor {
	port, err := getFreePort()
	if err != nil {
		tb.Fatal(err)
	}
	opts := &server.Options{
		JetStream: true,
		Port:      port,
		StoreDir:  tb.TempDir(),
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		tb.Fatal(err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(4 * time.Second) {
		tb.Fatal("server not ready for connections")
	}

	params := &Parameters{
		ClientURL: ns.ClientURL(),
	}

	// params := &Parameters{
	// 	ClientURL: "127.0.0.1:4222",
	// }

	return func() (storagedriver.StorageDriver, error) {
		return New(context.Background(), params)
	}
}

func TestNATSDriverSuite(t *testing.T) {
	testsuites.Driver(t, newDriverConstructor(t))
	ns.Shutdown()
}

func BenchmarkNATSDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newDriverConstructor(b))
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
