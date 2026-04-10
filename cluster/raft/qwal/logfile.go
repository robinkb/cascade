package qwal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"syscall"
)

var logNameFmt = "%020d.log"
var logNameRe = regexp.MustCompile(`(\d{20}).log`)

func createLogFile(dir string, id LogID, size int64) (*logFile, error) {
	filename := filepath.Join(dir, fmt.Sprintf(logNameFmt, id))

	w, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	err = syscall.Fallocate(int(w.Fd()), 0, 0, size)
	if err != nil {
		return nil, err
	}

	r, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &logFile{
		ID:     id,
		Writer: w,
		Reader: r,
		log:    newLog(r, w),
	}, nil
}

func openLogFile(filename string) (*logFile, error) {
	basename := filepath.Base(filename)
	id, err := strconv.ParseUint(basename[:len(basename)-4], 10, 64)
	if err != nil {
		return nil, err
	}

	w, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	r, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &logFile{
		ID:     LogID(id),
		Writer: w,
		Reader: r,
		log:    newLog(r, w),
	}, nil
}

// discoverLogFiles searches for files with names matching logNameRe in directory dir,
// returning a sorted list of filenames.
func discoverLogFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	logFiles := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		if logNameRe.MatchString(e.Name()) {
			logFiles = append(logFiles, e.Name())
		}
	}

	slices.Sort(logFiles)
	return logFiles, nil
}

// logFile represents a Log that is backed by a file.
// It wraps the basic Log and adds an ID for tracking unique Logs,
// and the handles of the underlying file.
type logFile struct {
	*log
	ID     LogID
	Writer *os.File
	Reader *os.File
}

func (l *logFile) Sync() error {
	return syscall.Fdatasync(int(l.Writer.Fd()))
}

func (l *logFile) Lock() error {
	if err := l.Sync(); err != nil {
		return err
	}

	if err := os.Truncate(l.Writer.Name(), l.cursor); err != nil {
		return err
	}

	return l.Writer.Close()
}

func (l *logFile) Close() error {
	if err := l.Writer.Close(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			return err
		}
	}
	return l.Reader.Close()
}
