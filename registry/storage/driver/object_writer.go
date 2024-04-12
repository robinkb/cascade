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
	"bytes"
	"context"
	"errors"
	"fmt"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/nats-io/nats.go/jetstream"
)

func newObjectWriter(ctx context.Context, store jetstream.ObjectStore, name string, append bool) (*objectWriter, error) {
	fw := &objectWriter{
		ctx:  ctx,
		obs:  store,
		name: name,
		buf:  bytes.NewBuffer(make([]byte, 0, 32*1024*1024)),
	}

	if append {
		info, err := fw.obs.GetInfo(fw.ctx, fw.name)
		if err == nil && !isLink(info) {
			return nil, errors.New("file already exists and is not a link")
		}

		for {
			info, err := fw.obs.GetInfo(fw.ctx, fmt.Sprintf("%s/%d", fw.name, fw.index))
			if errors.Is(err, jetstream.ErrObjectNotFound) {
				break
			}
			if err != nil {
				return nil, err
			}
			fw.index++
			fw.size += int64(info.Size)
		}
	}

	return fw, nil
}

type objectWriter struct {
	ctx  context.Context
	obs  jetstream.ObjectStore
	name string

	buf   *bytes.Buffer
	index int
	size  int64
}

// Make sure that we satisfy the interface.
var _ storagedriver.FileWriter = &objectWriter{}

func (obw *objectWriter) Write(data []byte) (int, error) {
	// n is the amount of bytes written during this Write call
	var n int
	// w is the bytes written in a loop
	var w int
	for {
		if obw.buf.Available() < len(data)-n {
			w, _ = obw.buf.Write(data[n : n+obw.buf.Available()])
		} else {
			w, _ = obw.buf.Write(data[n:])
		}
		n += w

		// Add chunk if the buffer is full
		if obw.buf.Available() == 0 {
			err := obw.flush()
			if err != nil {
				return 0, err
			}
		}

		if len(data) == n {
			break
		}
	}

	return w, nil
}

func (obw *objectWriter) flush() error {
	meta := jetstream.ObjectMeta{
		Name: fmt.Sprintf("%s/%d", obw.name, obw.index),
		Opts: &jetstream.ObjectMetaOptions{
			ChunkSize: defaultChunkSize,
		},
	}

	info, err := obw.obs.Put(obw.ctx, meta, obw.buf)
	if err != nil {
		return err
	}
	obw.index++
	obw.size += int64(info.Size)
	obw.buf.Reset()

	return nil
}

func (obw *objectWriter) Close() error {
	if obw.buf.Len() != 0 {
		return obw.flush()
	}
	return nil
}

// Size returns the number of bytes written to this FileWriter.
func (obw *objectWriter) Size() int64 {
	return obw.size
}

// Cancel removes any written content from this FileWriter.
func (obw *objectWriter) Cancel(ctx context.Context) error {
	errs := make([]error, 0)
	for i := 0; i < obw.index; i++ {
		err := obw.obs.Delete(ctx, fmt.Sprintf("%s/%d", obw.name, i))
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		errs = append([]error{errors.New("failed to cancel upload")}, errs...)
		return errors.Join(errs...)
	}

	return nil
}

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver.GetContent and
// StorageDriver.Reader.
func (obw *objectWriter) Commit(context.Context) error {
	if err := obw.flush(); err != nil {
		return err
	}

	info, err := obw.obs.GetInfo(obw.ctx, fmt.Sprintf("%s/%d", obw.name, 0))
	if err != nil {
		return err
	}

	// Already checked that the file is safe to delete.
	err = obw.obs.Delete(obw.ctx, obw.name)
	if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
		return err
	}

	_, err = obw.obs.AddLink(obw.ctx, obw.name, info)
	return err
}
