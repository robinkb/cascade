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
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	driverName = "nats"

	sep = "/"

	defaultChunkSize = 1 * 1024 * 1024
)

// Ensure that we satisfy the interface.
var _ storagedriver.StorageDriver = &driver{}

type driver struct {
	js   jetstream.JetStream
	root jetstream.ObjectStore
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.Storagedriver implementation backed by NATS JetStream.
type Driver struct {
	baseEmbed
}

func init() {
	factory.Register(driverName, &natsDriverFactory{})
}

type natsDriverFactory struct{}

func (factory *natsDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(ctx, parameters)
}

// New constructs a new Driver
func New(ctx context.Context, params *Parameters) (*Driver, error) {
	js, err := newJetStream(params)
	if err != nil {
		return nil, err
	}

	// The root store is a special bucket from which the directory tree begins.
	config := jetstream.ObjectStoreConfig{
		Bucket:      "root",
		Description: "/",
	}
	root, err := js.CreateOrUpdateObjectStore(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure root store exists: %w", err)
	}

	d := &driver{js, root}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				// TODO: Figure out why concurrency is a problem,
				// and probably make this configurable.
				StorageDriver: base.NewRegulator(d, 1),
			},
		},
	}, nil
}

// Name returns the human-readable "name" of the driver, useful in error
// messages and logging. By convention, this will just be the registration
// name, but drivers may provide other information here.
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	dir, file := filepath.Split(path)
	workingStore, err := d.findBucket(ctx, dir)
	if err != nil {
		return nil, err
	}

	data, err := workingStore.GetBytes(ctx, file)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get content '%s': %w", path, err)
	}
	return data, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	dir, file := filepath.Split(path)
	workingStore, err := d.makeBucket(ctx, dir)
	if err != nil {
		return err
	}

	_, err = workingStore.PutBytes(ctx, file, content)
	if err != nil {
		return err
	}

	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	dir, file := filepath.Split(path)
	workingStore, err := d.findBucket(ctx, dir)
	if err != nil {
		return nil, err
	}

	// TODO: Specify offset if store.Get ever supports it.
	obj, err := workingStore.Get(ctx, file)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	if err != nil {
		return nil, fmt.Errorf("unexpected error getting reader for path '%s': %w", path, err)
	}

	// TODO: Can cut all this once jetstream.ObjectStore.Get supports specifying offset.
	reader := bufio.NewReader(obj)
	// Will this be a problem...?
	_, err = reader.Discard(int(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to skip in file '%s' to offset %d: %w", path, offset, err)
	}

	return io.NopCloser(reader), nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
// A path may be appended to if it has not been committed, or if the
// existing committed content is zero length.
//
// The behaviour of appending to paths with non-empty committed content is
// undefined. Specific implementations may document their own behavior.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	dir, file := filepath.Split(path)
	workingStore, err := d.makeBucket(ctx, dir)
	if err != nil {
		return nil, err
	}

	meta := jetstream.ObjectMeta{
		Name: file,
		Opts: &jetstream.ObjectMetaOptions{
			ChunkSize: defaultChunkSize,
		},
	}

	return newFileWriter(ctx, workingStore, meta, append)
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	// Root directory is a special case, because it is the only path
	// allowed to end with a slash. We're still getting the info from
	// the backend because the storage health check calls Stat("/"),
	// and we should actually try to call the backend.
	if path == "/" {
		_, err := d.root.Status(ctx)
		return &FileInfo{path: path, dir: true}, err
	}

	dir, file := filepath.Split(path)
	workingStore, err := d.findBucket(ctx, dir)
	if err != nil {
		return nil, err
	}

	info, err := workingStore.GetInfo(ctx, file)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	if err != nil {
		return nil, err
	}

	if isDirectory(info) {
		return &FileInfo{
			path: path,
			dir:  true,
		}, nil
	}

	return &FileInfo{
		path:    path,
		size:    int64(info.Size),
		modTime: info.ModTime,
	}, nil
}

// List returns a list of the objects that are direct descendants of the
// given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	workingStore, err := d.findBucket(ctx, path)
	if err != nil {
		return nil, err
	}

	objs, err := workingStore.List(ctx)
	if errors.Is(err, jetstream.ErrNoObjectsFound) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}

	files := make([]string, len(objs))
	for i := range objs {
		files[i] = filepath.Join(path, objs[i].Name)
	}

	return files, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
// Note: This may be no more efficient than a copy followed by a delete for
// many implementations.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	sourceDir, sourceFile := filepath.Split(sourcePath)
	sourceWorkingStore, err := d.findBucket(ctx, sourceDir)
	if err != nil {
		return err
	}

	sourceObj, err := sourceWorkingStore.Get(ctx, sourceFile)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}
	if err != nil {
		return fmt.Errorf("unexpected error getting reader for path '%s': %w", sourcePath, err)
	}

	destDir, destFile := filepath.Split(destPath)

	// Fast path when source and dest are in the same bucket: rename the object.
	if sourceDir == destDir {
		err := sourceWorkingStore.UpdateMeta(ctx, sourceFile, jetstream.ObjectMeta{
			Name: destFile,
		})
		if err != nil {
			return fmt.Errorf("failed to move '%s' to '%s': %w", sourcePath, destPath, err)
		}
		return nil
	}

	destWorkingStore, err := d.makeBucket(ctx, destDir)
	if err != nil {
		return err
	}

	meta := jetstream.ObjectMeta{Name: destFile}
	_, err = destWorkingStore.Put(ctx, meta, sourceObj)
	if err != nil {
		return err
	}

	if err := sourceWorkingStore.Delete(ctx, sourceFile); err != nil {
		return fmt.Errorf("failed to delete source file '%s' after move operation: %w", sourcePath, err)
	}

	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	dir, file := filepath.Split(path)
	workingStore, err := d.findBucket(ctx, dir)
	if err != nil {
		return err
	}

	info, err := workingStore.GetInfo(ctx, file)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return storagedriver.PathNotFoundError{}
	}
	if err != nil {
		return err
	}

	// If it's a directory, we must recurse into it and delete it.
	if isDirectory(info) {
		if err := d.deleteBucket(ctx, info.Opts.Link.Bucket); err != nil {
			return err
		}
	}

	err = workingStore.Delete(ctx, info.Name)
	if err != nil {
		return err
	}

	return nil
}

// RedirectURL returns a URL which the client of the request r may use
// to retrieve the content stored at path. Returning the empty string
// signals that the request may not be redirected.
func (d *driver) RedirectURL(r *http.Request, path string) (string, error) {
	// NATS doesn't have an HTTP interface, so... doesn't make sense.
	return "", nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file.
// If the returned error from the WalkFn is ErrSkipDir and fileInfo refers
// to a directory, the directory will not be entered and Walk
// will continue the traversal.
// If the returned error from the WalkFn is ErrFilledBuffer, processing stops.
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	// TODO: Should I implement something custom?
	return storagedriver.WalkFallback(ctx, d, path, f, options...)
}

// findBucket retrieves the object store backing the given path.
func (d *driver) findBucket(ctx context.Context, path string) (jetstream.ObjectStore, error) {
	if path == "/" {
		return d.root, nil
	}

	path = strings.TrimRight(path, sep)
	hash := hashPath(path)

	store, err := d.js.ObjectStore(ctx, hash)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return store, err
}

// makeBucket finds or creates object stores to back the given path.
func (d *driver) makeBucket(ctx context.Context, path string) (jetstream.ObjectStore, error) {
	store, err := d.findBucket(ctx, path)
	if err == nil {
		return store, nil
	}

	path = strings.TrimRight(path, "/")
	parts := strings.Split(path, sep)

	workingStore := d.root
	// Tracking working dir to construct bucket names.
	// Might be more efficient to keep this in a slice or something.
	workingDir := ""
	for i := 1; i < len(parts); i++ {
		currentDir := strings.Join([]string{workingDir, parts[i]}, sep)
		config := jetstream.ObjectStoreConfig{
			Bucket:      hashPath(currentDir),
			Description: currentDir,
		}
		bucket, err := d.js.CreateOrUpdateObjectStore(ctx, config)
		if err != nil {
			return nil, err
		}
		_, err = workingStore.AddBucketLink(ctx, parts[i], bucket)
		if err != nil {
			return nil, err
		}
		workingStore = bucket
		workingDir = currentDir
	}

	return workingStore, nil
}

// deleteBucket recursively removes all buckets under the given bucket.
func (d *driver) deleteBucket(ctx context.Context, bucket string) error {
	store, err := d.js.ObjectStore(ctx, bucket)
	if err != nil {
		return err
	}

	objs, err := store.List(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
		return err
	}

	for i := range objs {
		if isDirectory(objs[i]) {
			if err := d.deleteBucket(ctx, objs[i].Opts.Link.Bucket); err != nil {
				return err
			}
		} else {
			// TODO: Deleting files ourselves is probably not necessary.
			// NATS should clean them up when the bucket gets deleted.
			if err := store.Delete(ctx, objs[i].Name); err != nil {
				return err
			}
		}
	}

	return d.js.DeleteObjectStore(ctx, bucket)
}

func newJetStream(params *Parameters) (jetstream.JetStream, error) {
	nc, err := nats.Connect(params.ClientURL)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	return js, err
}

func hashPath(path string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(path)))
}

func isDirectory(info *jetstream.ObjectInfo) bool {
	return info.Opts.Link != nil && info.Opts.Link.Bucket != ""
}
