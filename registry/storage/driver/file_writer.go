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

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/nats-io/nats.go/jetstream"
)

func newFileWriter(ctx context.Context, store jetstream.ObjectStore, meta jetstream.ObjectMeta, append bool) (*FileWriter, error) {
	var obw jetstream.ObjectStoreWriter
	var err error
	if !append {
		obw, err = store.Writer(ctx, meta)
	} else {
		obw, err = store.AppendWriter(ctx, meta)
	}

	return &FileWriter{
		ctx:  ctx,
		name: meta.Name,
		obw:  obw,
	}, err
}

type FileWriter struct {
	ctx  context.Context
	obs  jetstream.ObjectStore
	name string
	obw  jetstream.ObjectStoreWriter
}

var _ storagedriver.FileWriter = &FileWriter{}

func (f *FileWriter) Write(data []byte) (int, error) {
	n, err := f.obw.Write(data)
	return n, err
}

func (f *FileWriter) Close() error {
	return f.obw.Close()
}

// Size returns the number of bytes written to this FileWriter.
func (f *FileWriter) Size() int64 {
	return int64(f.obw.Size())
}

// Cancel removes any written content from this FileWriter.
func (f *FileWriter) Cancel(ctx context.Context) error {
	return f.obs.Delete(ctx, f.name)
}

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver.GetContent and
// StorageDriver.Reader.
func (f *FileWriter) Commit(context.Context) error {
	return nil
}
