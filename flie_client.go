package treeply

import (
	"io"
	"log"
	"strings"
)

// Intended to be used by a single thread
type FileClient struct {
	FileService *FileService

	FileHandles map[int]*FileHandle

	nextFileHandle  int
	freeFileHandles []int
}

type FileClientDiagnostics struct {
	FileService     *FileServiceDiagnostics
	OpenFiles       int
	FreeFileHandles int
}

func (f *FileClient) GetDiagnostics() *FileClientDiagnostics {
	return &FileClientDiagnostics{
		FileService:     f.FileService.GetDiagnostics(),
		OpenFiles:       len(f.FileHandles),
		FreeFileHandles: len(f.freeFileHandles),
	}
}

func NewFileClient(fs *FileService) *FileClient {
	return &FileClient{FileService: fs, FileHandles: make(map[int]*FileHandle)}
}

type FileHandle struct {
	INode  INode
	Offset int64
}

type FileClientDirEntry struct {
	Name  string
	Size  int64
	INode INode
	IsDir bool
}

func (f *FileService) GetINodeForPath(path string) (INode, error) {
	inode := f.Root
	refcount := f.INodes.UpdateRefCount(inode, 1)
	log.Printf("GetINodeForPath start root = %d, incrementing refcount -> %d", inode, refcount)

	if path == "" {
		return inode, nil
	}

	log.Printf("GetINodeForPath 3")
	components := strings.Split(path, "/")
	for _, component := range components {
		var err error
		prevINode := inode
		inode, err = f.INodes.LookupInDirWithErr(inode, component)
		refcount = f.INodes.UpdateRefCount(prevINode, -1)
		log.Printf("GetINodeForPath 4 inode %d (%s) refcount decremented -> %d", inode, component, refcount)
		if err != nil {
			return 0, err
		}
		log.Printf("GetINodeForPath 5")
	}

	log.Printf("GetINodeForPath 6")
	return inode, nil
}

func (fc *FileClient) GetINodeForPath(path string) (INode, error) {
	return fc.FileService.GetINodeForPath(path)
}

func (fc *FileClient) Forget(req *ForgetReq) (*CloseResp, error) {
	err := fc.FileService.Forget(req.Path)
	if err != nil {
		return nil, err
	}

	return &CloseResp{}, nil
}

func (fc *FileClient) ListDir(req *ListDirReq) (*ListDirResp, error) {
	path := req.Path
	inode, err := fc.GetINodeForPath(path)
	if err != nil {
		return nil, err
	}

	defer fc.FileService.INodes.UpdateRefCount(inode, -1)

	log.Printf("ListDir p2")
	dirEntries, err := fc.FileService.INodes.ReadDirWithErr(inode)
	if err != nil {
		return nil, err
	}

	fcde := make([]FileClientDirEntry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		fcde = append(fcde, FileClientDirEntry{Name: dirEntry.Name, Size: dirEntry.Size, INode: dirEntry.INode, IsDir: dirEntry.IsDir})
	}

	log.Printf("ListDir end")
	return &ListDirResp{Entries: fcde}, nil
}

const INVALID_FD = -1

type Response interface{}

func (fc *FileClient) Open(req *OpenReq) (*OpenResp, error) {
	inode, err := fc.GetINodeForPath(req.Path)
	if err != nil {
		return nil, err
	}

	dirEntry, err := fc.FileService.INodes.Stat(inode)
	if err != nil {
		return nil, err
	}

	if dirEntry.IsDir {
		fc.FileService.INodes.UpdateRefCount(inode, -1)
		return nil, IS_DIR
	}

	var fd int
	if len(fc.freeFileHandles) == 0 {
		fd = fc.nextFileHandle
		fc.nextFileHandle++
	} else {
		fd = fc.freeFileHandles[len(fc.freeFileHandles)-1]
		fc.freeFileHandles = fc.freeFileHandles[:len(fc.freeFileHandles)-1]
	}
	fc.FileHandles[fd] = &FileHandle{INode: inode, Offset: 0}

	return &OpenResp{FD: fd}, nil
}

func (fc *FileClient) Close(req *CloseReq) (*CloseResp, error) {
	fh, ok := fc.FileHandles[req.FD]
	if !ok {
		return nil, INVALID_HANDLE
	}

	delete(fc.FileHandles, req.FD)
	fc.freeFileHandles = append(fc.freeFileHandles, req.FD)

	fc.FileService.INodes.UpdateRefCount(fh.INode, -1)
	return &CloseResp{}, nil
}

func (fc *FileClient) Read(req *ReadReq) (*ReadResp, error) {
	fh, ok := fc.FileHandles[req.FD]
	if !ok {
		return nil, INVALID_HANDLE
	}

	buffer := make([]byte, req.Length)
	n, err := fc.FileService.INodes.ReadFile(fh.INode, fh.Offset, buffer)
	if err != nil && err != io.EOF {
		return nil, err
	}
	fh.Offset += int64(n)

	return &ReadResp{Data: buffer[:n]}, nil
}
