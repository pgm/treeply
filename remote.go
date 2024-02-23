package treeply

import (
	"context"
	"io"
	"log"
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
	GetDiagnostics() interface{}
}

type DirRemoteProvider struct {
	Root            string
	DirListingDelay time.Duration
	ReadDelay       time.Duration
}

type DirRemoteProviderDiagnostics struct {
	Root string
}

func (d *DirRemoteProvider) GetDiagnostics() interface{} {
	return &DirRemoteProviderDiagnostics{Root: d.Root}
}

func (d *DirRemoteProvider) GetDirListing(ctx context.Context, path string) ([]RemoteFile, error) {
	time.Sleep(d.DirListingDelay)

	log.Printf("GetDirListing(%s)", path)
	if path == "" {
		path = d.Root
	} else {
		path = d.Root + "/" + path
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	log.Printf("ReadDir(%s) -> %s", path, entries)
	result := make([]RemoteFile, 0, len(entries))
	for _, entry := range entries {
		fi, err := entry.Info()
		if err != nil {
			return nil, err
		}
		name := entry.Name()
		// if fi.IsDir() {
		// 	if !strings.HasSuffix(name, "/") {
		// 		log.Fatalf("dir didn't end with /: %s", name)
		// 	}
		// 	name = name[:len(name)-1]
		// }
		result = append(result, RemoteFile{Name: name, IsDir: fi.IsDir(), ETag: fi.ModTime().String(), Size: fi.Size()})
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
