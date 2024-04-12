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

func newObjectReader(ctx context.Context, obs jetstream.ObjectStore, filename string, offset int64) (*objectReader, error) {
	obr := &objectReader{
		ctx:      ctx,
		obs:      obs,
		filename: filename,
	}

	info, err := obs.GetInfo(ctx, filename)
	if err != nil {
		return nil, err
	}

	if !isMultipart(info) {
		obr.objs = 1
		obr.current, err = obs.Get(ctx, filename)
		if err != nil {
			return nil, err
		}

		if offset != 0 {
			if _, err := io.CopyN(io.Discard, obr.current, offset); err != nil {
				return nil, err
			}
		}
	} else {
		obr.objs, err = strconv.Atoi(info.Headers.Get(headerMultipartCount))
		if err != nil {
			return nil, fmt.Errorf("failed to parse multipart header: %w", err)
		}

		if offset == 0 {
			obr.current, err = obs.Get(ctx, fmt.Sprintf(multipartTemplate, filename, 0))
			if err != nil {
				return nil, err
			}
		} else {
			// An ObjectReader may consist of multiple parts.
			// When reading from an offset, we need to find in which part
			// the offset falls in, and start reading from there.
			// If the offset is greater than the multipart length,
			// this loop will ensure that len(objectReader.objs) <= objectReader.index,
			// and reads will return (0, io.EOF) as expected.
			var seek int64
			for i := 0; i < obr.objs; i++ {
				info, err := obs.GetInfo(ctx, fmt.Sprintf(multipartTemplate, filename, i))
				if err != nil {
					return nil, err
				}

				if seek+int64(info.Size) > offset {
					// Offset falls within this part. Read until the offset,
					// discarding any bytes found.
					obr.current, err = obs.Get(ctx, filename)
					if err != nil {
						return nil, err
					}

					if _, err := io.CopyN(io.Discard, obr.current, offset-seek); err != nil {
						return nil, err
					}
				} else {
					seek += int64(info.Size)
					obr.index++
				}
			}
		}
	}

	return obr, nil
}

type objectReader struct {
	ctx      context.Context
	obs      jetstream.ObjectStore
	filename string

	objs    int
	index   int
	current jetstream.ObjectResult

	errs []error
}

func (obr *objectReader) Read(p []byte) (n int, err error) {
	// Any attempts to read when all objects have already been read
	// should result in 0 bytes read and EOF.
	if obr.objs <= obr.index {
		return 0, io.EOF
	}

	n, err = obr.current.Read(p)

	if err == io.EOF {
		if err := obr.current.Close(); err != nil {
			obr.errs = append(obr.errs, err)
		}

		obr.index++
		// Open the next object for reading
		if obr.objs != obr.index {
			obr.current, err = obr.obs.Get(obr.ctx, fmt.Sprintf(multipartTemplate, obr.filename, obr.index))
			if err != nil {
				return n, err
			}
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
