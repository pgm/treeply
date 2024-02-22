package treeply

import (
	"errors"
	"io"
	"strings"
)

// Intended to be used by a single thread
type FileClient struct {
	FileService *FileService

	FileHandles map[int]*FileHandle

	nextFileHandle  int
	freeFileHandles []int
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
}

func (fc *FileClient) GetINodeForPath(path string) (INode, error) {
	inode := fc.FileService.Root
	fc.FileService.INodes.UpdateRefCount(inode, 1)

	if path == "" {
		return inode, nil
	}

	components := strings.Split(path, "/")
	for _, component := range components {
		var err error
		prevINode := inode
		inode, err = fc.FileService.INodes.LookupInDirWithErr(inode, component)
		fc.FileService.INodes.UpdateRefCount(prevINode, -1)
		if err != nil {
			return 0, err
		}
	}

	return inode, nil
}

func (fc *FileClient) ListDir(req *ListDirReq) (*ListDirResp, error) {
	path := req.Path
	inode, err := fc.GetINodeForPath(path)
	if err != nil {
		return nil, err
	}
	// todo fix this to also check for errors
	dirEntries := fc.FileService.INodes.ReadDir(inode)
	fc.FileService.INodes.UpdateRefCount(inode, -1)

	fcde := make([]FileClientDirEntry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		fcde = append(fcde, FileClientDirEntry{Name: dirEntry.Name, Size: dirEntry.Size, INode: dirEntry.INode})
	}

	return &ListDirResp{Entries: fcde}, nil
}

var INVALID_HANDLE = errors.New("Invalid handle")

const INVALID_FD = -1

type Response interface{}

func (fc *FileClient) Open(req *OpenReq) (*OpenResp, error) {
	inode, err := fc.GetINodeForPath(req.Path)
	if err != nil {
		return nil, INVALID_HANDLE
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
	fc.FileService.INodes.UpdateRefCount(fh.INode, -1)
	return &CloseResp{}, nil
}

func (fc *FileClient) Read(req *ReadReq) (*ReadResp, error) {
	fh, ok := fc.FileHandles[req.FD]
	if !ok {
		return nil, INVALID_HANDLE
	}

	buffer := make([]byte, req.Length)
	n, err := fc.FileService.INodes.ReadFile(fh.INode, uint64(fh.Offset), buffer)
	if err != nil && err != io.EOF {
		return nil, err
	}
	fh.Offset += int64(n)

	return &ReadResp{Data: buffer[:n]}, nil
}
