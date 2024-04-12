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
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

func newObjectReader(ctx context.Context, obs jetstream.ObjectStore, name string, offset int64) (*objectReader, error) {
	obr := &objectReader{
		ctx:  ctx,
		obs:  obs,
		name: name,
	}

	info, err := obs.GetInfo(ctx, name)
	if err != nil {
		return nil, err
	}

	if isMultipart(info) {
		parts, err := strconv.Atoi(info.Headers.Get(multipartHeader))
		if err != nil {
			return nil, fmt.Errorf("failed to parse multipart header: %w", err)
		}

		obr.objs = make([]jetstream.ObjectResult, parts)

		for i := 0; i < parts; i++ {
			obj, err := obs.Get(ctx, fmt.Sprintf(multipartTemplate, obr.name, i))
			if err != nil {
				return nil, err
			}
			obr.objs[i] = obj
		}
	} else {
		obj, err := obs.Get(ctx, name)
		if err != nil {
			return nil, err
		}
		obr.objs = []jetstream.ObjectResult{obj}
	}

	if offset != 0 {
		// An ObjectReader may consist of multiple parts.
		// When reading from an offset, we need to find in which part
		// the offset falls in, and start reading from there.
		// If the offset is greater than the multipart length,
		// this loop will ensure that len(objectReader.objs) <= objectReader.index,
		// and reads will return (0, io.EOF) as expected.
		var seek int64
		for _, obj := range obr.objs {
			info, err := obj.Info()
			if err != nil {
				return nil, err
			}

			if seek+int64(info.Size) > offset {
				// Offset falls within this part. Read until the offset,
				// discarding any bytes found.
				if _, err := io.CopyN(io.Discard, obj, offset-seek); err != nil {
					return nil, err
				}
			} else {
				// Offset does not fall within this part; skip and close it.
				seek += int64(info.Size)
				obr.index++
				if err := obj.Close(); err != nil {
					return nil, err
				}
			}
		}
	}

	return obr, nil
}

type objectReader struct {
	io.ReadCloser

	ctx  context.Context
	obs  jetstream.ObjectStore
	name string

	objs  []jetstream.ObjectResult
	index int

	errs []error
}

func (obr *objectReader) Read(p []byte) (n int, err error) {
	// Any attempts to read when all objects have already been read
	// should result in 0 bytes read and EOF.
	if len(obr.objs) <= obr.index {
		return 0, io.EOF
	}

	n, err = obr.objs[obr.index].Read(p)

	if err == io.EOF {
		if err := obr.objs[obr.index].Close(); err != nil {
			obr.errs = append(obr.errs, err)
		}

		obr.index++
		// If there are more objects to read, clear the EOF error
		if len(obr.objs) != obr.index {
			err = nil
		}
	}

	return n, err
}

func (obr *objectReader) Close() error {
	if len(obr.errs) > 0 {
		obr.errs = append([]error{errors.New("failed to close object")}, obr.errs...)
		return errors.Join(obr.errs...)
	}

	return nil
}
