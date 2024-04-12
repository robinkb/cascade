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

	"github.com/nats-io/nats.go/jetstream"
)

func newObjectReader(ctx context.Context, obs jetstream.ObjectStore, name string, offset int64) (*objectReader, error) {
	obr := &objectReader{
		ctx:  ctx,
		obs:  obs,
		name: name,
		objs: make([]jetstream.ObjectResult, 0),
	}

	info, err := obs.GetInfo(ctx, name)
	if err != nil {
		return nil, err
	}

	if isLink(info) {
		var index int
		for {
			obj, err := obs.Get(ctx, fmt.Sprintf("%s/%d", name, index))
			if errors.Is(err, jetstream.ErrObjectNotFound) {
				// No more parts to find.
				break
			}
			if err != nil {
				return nil, err
			}
			index++
			obr.objs = append(obr.objs, obj)
		}
	} else {
		obj, err := obs.Get(ctx, name)
		if err != nil {
			return nil, err
		}
		obr.objs = []jetstream.ObjectResult{obj}
	}

	if offset != 0 {
		var seek int64
		for i := range obr.objs {
			info, err := obr.objs[i].Info()
			if err != nil {
				return nil, err
			}

			if seek+int64(info.Size) > offset {
				io.CopyN(io.Discard, obr.objs[i], offset-seek)
			} else {
				seek += int64(info.Size)
				obr.index++
				continue
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
