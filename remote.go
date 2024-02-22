package treeply

import (
	"context"
	"io"
	"os"
	"time"
)

type RemoteFile struct {
	Name  string
	IsDir bool
	ETag  string
	Size  int64
}

type RemoteProvider interface {
	GetDirListing(ctx context.Context, path string) ([]RemoteFile, error)
	GetReader(ctx context.Context, path string, ETag string, Offset int64, Length int64) (io.Reader, error)
}

type DirRemoteProvider struct {
	Root            string
	DirListingDelay time.Duration
	ReadDelay       time.Duration
}

func (d *DirRemoteProvider) GetDirListing(ctx context.Context, path string) ([]RemoteFile, error) {
	time.Sleep(d.DirListingDelay)

	if path == "" {
		path = d.Root
	} else {
		path = d.Root + "/" + path
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	result := make([]RemoteFile, 0, len(entries))
	for _, entry := range entries {
		fi, err := entry.Info()
		if err != nil {
			return nil, err
		}
		result = append(result, RemoteFile{Name: entry.Name(), IsDir: fi.IsDir(), ETag: fi.ModTime().String(), Size: fi.Size()})
	}
	return result, nil
}

type BoundedReader struct {
	bytesRemaining int64
	reader         io.ReadCloser
}

func (b *BoundedReader) Read(buffer []byte) (int, error) {
	if len(buffer) > int(b.bytesRemaining) {
		buffer = buffer[:b.bytesRemaining]
	}
	n, err := b.reader.Read(buffer)
	b.bytesRemaining -= int64(n)
	if err != nil {
		return n, err
	}
	if b.bytesRemaining == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (b *BoundedReader) Close() error {
	return b.reader.Close()
}

func (d *DirRemoteProvider) GetReader(ctx context.Context, path string, ETag string, Offset int64, Length int64) (io.Reader, error) {
	time.Sleep(d.ReadDelay)

	if path == "" {
		path = d.Root
	} else {
		path = d.Root + "/" + path
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &BoundedReader{reader: f, bytesRemaining: Length}, nil
}
