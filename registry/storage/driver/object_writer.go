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
	"strconv"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	headerMultipartCount = "Cascade-Registry-Multipart-Count"
	headerMultipartSize  = "Cascade-Registry-Multipart-Size"
	multipartTemplate    = "%s/%d"

	writeBufferSize  = 64 * 1024 * 1024
	defaultChunkSize = 1 * 1024 * 1024
)

func newObjectWriter(ctx context.Context, obs jetstream.ObjectStore, filename string, append bool) (*objectWriter, error) {
	fw := &objectWriter{
		ctx:      ctx,
		obs:      obs,
		filename: filename,
		buf:      bytes.NewBuffer(make([]byte, 0, writeBufferSize)),
	}

	if append {
		info, err := fw.obs.GetInfo(ctx, filename)
		if err != nil {
			return nil, err
		}
		if !isMultipart(info) {
			return nil, errors.New("file already exists and is not a multipart file")
		}

		parts, err := strconv.Atoi(info.Headers.Get(headerMultipartCount))
		if err != nil {
			return nil, fmt.Errorf("failed to parse multipart header: %w", err)
		}

		for i := 0; i < parts; i++ {
			info, err := fw.obs.GetInfo(ctx, fmt.Sprintf(multipartTemplate, filename, i))
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
	ctx      context.Context
	obs      jetstream.ObjectStore
	filename string

	buf   *bytes.Buffer
	index int
	size  int64

	committed bool
	cancelled bool
	closed    bool
}

// Make sure that we satisfy the interface.
var _ storagedriver.FileWriter = &objectWriter{}

func (obw *objectWriter) Write(data []byte) (int, error) {
	if obw.closed {
		return 0, fmt.Errorf("already closed")
	} else if obw.committed {
		return 0, fmt.Errorf("already committed")
	} else if obw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

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
		Name: fmt.Sprintf(multipartTemplate, obw.filename, obw.index),
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
	if obw.closed {
		return fmt.Errorf("already closed")
	}
	obw.closed = true

	if err := obw.flush(); err != nil {
		return err
	}

	headers := nats.Header{}
	headers.Set(headerMultipartCount, strconv.Itoa(obw.index))
	headers.Set(headerMultipartSize, strconv.FormatInt(obw.size, 10))

	meta := jetstream.ObjectMeta{
		Name:    obw.filename,
		Headers: headers,
	}
	_, err := obw.obs.Put(obw.ctx, meta, bytes.NewReader(nil))
	return err
}

// Size returns the number of bytes written to this FileWriter.
func (obw *objectWriter) Size() int64 {
	return obw.size
}

// Cancel removes any written content from this FileWriter.
func (obw *objectWriter) Cancel(ctx context.Context) error {
	if obw.closed {
		return fmt.Errorf("already closed")
	} else if obw.committed {
		return fmt.Errorf("already committed")
	}
	obw.cancelled = true

	errs := make([]error, 0)
	for i := 0; i < obw.index; i++ {
		err := obw.obs.Delete(ctx, fmt.Sprintf(multipartTemplate, obw.filename, i))
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
//
// Having a separate commit call does not really make sense for my implementation.
func (obw *objectWriter) Commit(context.Context) error {
	if obw.closed {
		return fmt.Errorf("already closed")
	} else if obw.committed {
		return fmt.Errorf("already committed")
	} else if obw.cancelled {
		return fmt.Errorf("already cancelled")
	}
	obw.committed = true

	return nil
}

func isMultipart(info *jetstream.ObjectInfo) bool {
	return info.Size == 0 && info.Headers.Get(headerMultipartCount) != ""
}
