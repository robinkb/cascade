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

func newFileWriter(ctx context.Context, store jetstream.ObjectStore, name string, append bool) (*FileWriter, error) {
	fw := &FileWriter{
		ctx:  ctx,
		obs:  store,
		name: name,
		buf:  bytes.NewBuffer(make([]byte, 0, 32*1024*1024)),
	}

	if append {
		info, err := fw.obs.GetInfo(fw.ctx, fw.name)
		if err == nil && info.Size != 0 {
			return nil, errors.New("file already exists and is not zero-length")
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

type FileWriter struct {
	ctx  context.Context
	obs  jetstream.ObjectStore
	name string

	buf   *bytes.Buffer
	index int
	size  int64
}

var _ storagedriver.FileWriter = &FileWriter{}

func (f *FileWriter) Write(data []byte) (int, error) {
	// n is the amount of bytes written during this Write call
	var n int
	// w is the bytes written in a loop
	var w int
	for {
		if f.buf.Available() < len(data)-n {
			w, _ = f.buf.Write(data[n : n+f.buf.Available()])
		} else {
			w, _ = f.buf.Write(data[n:])
		}
		n += w

		// Add chunk if the buffer is full
		if f.buf.Available() == 0 {
			err := f.flush()
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

func (f *FileWriter) flush() error {
	meta := jetstream.ObjectMeta{
		Name: fmt.Sprintf("%s/%d", f.name, f.index),
		Opts: &jetstream.ObjectMetaOptions{
			ChunkSize: defaultChunkSize,
		},
	}

	info, err := f.obs.Put(f.ctx, meta, f.buf)
	if err != nil {
		return err
	}
	f.index++
	f.size += int64(info.Size)
	f.buf.Reset()

	return nil
}

func (f *FileWriter) Close() error {
	if f.buf.Len() != 0 {
		return f.flush()
	}
	return nil
}

// Size returns the number of bytes written to this FileWriter.
func (f *FileWriter) Size() int64 {
	return f.size
}

// Cancel removes any written content from this FileWriter.
func (f *FileWriter) Cancel(ctx context.Context) error {
	errs := make([]error, 0)
	for i := 0; i < f.index; i++ {
		err := f.obs.Delete(ctx, fmt.Sprintf("%s/%d", f.name, i))
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
func (f *FileWriter) Commit(context.Context) error {
	if err := f.flush(); err != nil {
		return err
	}

	info, err := f.obs.GetInfo(f.ctx, fmt.Sprintf("%s/%d", f.name, 0))
	if err != nil {
		return err
	}

	// Already checked that the file is safe to delete.
	err = f.obs.Delete(f.ctx, f.name)
	if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
		return err
	}

	_, err = f.obs.AddLink(f.ctx, f.name, info)
	return err
}
