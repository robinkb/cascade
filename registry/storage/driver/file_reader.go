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

func NewFileReader(ctx context.Context, obs jetstream.ObjectStore, name string, offset int64) (*FileReader, error) {
	fr := &FileReader{
		ctx:  ctx,
		obs:  obs,
		name: name,
		objs: make([]jetstream.ObjectResult, 0),
	}

	info, err := obs.GetInfo(fr.ctx, fr.name)
	if err != nil {
		return nil, err
	}

	if isLink(info) {
		var index int
		for {
			obj, err := obs.Get(ctx, fmt.Sprintf("%s/%d", fr.name, index))
			if errors.Is(err, jetstream.ErrObjectNotFound) {
				// No more parts to find.
				break
			}
			if err != nil {
				return nil, err
			}
			index++
			fr.objs = append(fr.objs, obj)
		}
	} else {
		obj, err := obs.Get(ctx, name)
		if err != nil {
			return nil, err
		}
		fr.objs = []jetstream.ObjectResult{obj}
	}

	if offset != 0 {
		var seek int64
		for i := range fr.objs {
			info, err := fr.objs[i].Info()
			if err != nil {
				return nil, err
			}

			if seek+int64(info.Size) > offset {
				io.CopyN(io.Discard, fr.objs[i], offset-seek)
			} else {
				seek += int64(info.Size)
				fr.index++
				continue
			}
		}
	}

	return fr, nil
}

type FileReader struct {
	io.ReadCloser

	ctx  context.Context
	obs  jetstream.ObjectStore
	name string

	objs  []jetstream.ObjectResult
	index int

	errs []error
}

func (fr *FileReader) Read(p []byte) (n int, err error) {
	if fr.index == len(fr.objs) {
		return 0, io.EOF
	}

	n, err = fr.objs[fr.index].Read(p)

	if err == io.EOF {
		err = fr.objs[fr.index].Close()
		if err != nil {
			fr.errs = append(fr.errs, err)
		}

		fr.index++
		if fr.index == len(fr.objs) {
			return 0, io.EOF
		}
	}

	return n, err
}

func (fr *FileReader) Close() error {
	if len(fr.errs) > 0 {
		fr.errs = append([]error{errors.New("failed to close object")}, fr.errs...)
		return errors.Join(fr.errs...)
	}

	return nil
}
