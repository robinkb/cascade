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
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

type FileInfo struct {
	path    string
	size    int64
	modTime time.Time
	dir     bool
}

var _ storagedriver.FileInfo = &FileInfo{}

// Path provides the full path of the target of this file info.
func (f *FileInfo) Path() string {
	return f.path
}

// Size returns current length in bytes of the file. The return value can
// be used to write to the end of the file at path. The value is
// meaningless if IsDir returns true.
func (f *FileInfo) Size() int64 {
	return f.size
}

// ModTime returns the modification time for the file. For backends that
// don't have a modification time, the creation time should be returned.
func (f *FileInfo) ModTime() time.Time {
	return f.modTime
}

// IsDir returns true if the path is a directory.
func (f *FileInfo) IsDir() bool {
	return f.dir
}
