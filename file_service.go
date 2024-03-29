package treeply

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"strings"
)

type FileService struct {
	Remote               RemoteProvider
	INodes               *INodes
	Root                 INode
	TransferServiceQueue chan interface{}
}

type FileServiceDiagnostics struct {
	Remote                interface{}
	INodes                interface{}
	TransferServiceStatus interface{}
}

func (f *FileService) GetDiagnostics() *FileServiceDiagnostics {
	response := make(chan *TransferServiceStatus)
	f.TransferServiceQueue <- &DiagnosticRequest{Response: response}
	transferServiceStatus := <-response

	return &FileServiceDiagnostics{
		Remote:                f.Remote.GetDiagnostics(),
		INodes:                f.INodes.GetDiagnostics(),
		TransferServiceStatus: transferServiceStatus,
	}
}

func (f *FileService) Forget(path string) error {
	oldINode, err := f.GetINodeForPath(path)
	if err != nil {
		return err
	}

	newINode, err := f.INodes.CloneINodeDir(oldINode)
	if err != nil {
		return err
	}

	if path == "" || path == "." {
		// special case: We're updating the root directory
		oldRoot := f.Root
		f.Root = newINode
		f.INodes.UpdateRefCount(oldRoot, -1)
	} else {
		// otherwise we need to update the parent dir
		parentDir := filepath.Dir(path)
		name := filepath.Base(path)
		parentINode, err := f.GetINodeForPath(parentDir)
		if err != nil {
			return err
		}
		f.INodes.SetDirEntry(parentINode, name, newINode)
	}

	return nil
}

func pathConcat(base string, name string) string {
	var result string
	if name == "" {
		result = base
	} else if base == "" {
		result = name
	} else if base == "gs://" {
		result = base + name
	} else {
		result = base + "/" + name
	}

	resultToCheck := strings.TrimPrefix(result, "gs://")

	for _, component := range strings.Split(resultToCheck, "/") {
		if component == "" || component == "." || component == ".." {
			log.Panicf("Invalid result from pathConcat(%s, %s) => %s", base, name, result)
		}
	}
	return result
}

func NewFileService(Remote RemoteProvider, WorkDir string, BlockSize int) (*FileService, error) {
	inodes, err := NewINodes(WorkDir, int(BlockSize))
	if err != nil {
		return nil, err
	}

	transferServiceQueue := make(chan interface{})
	fs := &FileService{Remote: Remote, INodes: inodes, TransferServiceQueue: transferServiceQueue}

	go TransferService(transferServiceQueue, inodes)

	// TODO: These implementations cause a fetch to always happen. This means
	// that there's race conditions that can happen (ie: two threads ask for
	// the same block) where we could make a single transfer as opposed to two.
	// So, functionally, correct, but may be inefficient. Probably could add
	// logging in SetBlocks to warn every time it sees we're replacing an
	// existing block with a new one.

	makeRequestCallback := func(path string, etag string) RequestCallback {
		requestCallback := func(inode INode, blockIndices []int) {
			Responses := make([]chan error, 0, len(blockIndices))

			log.Printf("Requesting %d blocks", len(blockIndices))
			for _, blockIndex := range blockIndices {
				Response := make(chan error)
				Responses = append(Responses, Response)

				offset := int64(blockIndex) * int64(BlockSize)
				length := int64(BlockSize)

				transferServiceQueue <- &BlockRequest{Block: INodeBlock{INode: inode, BlockIndex: blockIndex},
					GetReader: func(ctx context.Context) (io.Reader, error) {
						return Remote.GetReader(ctx, path, etag, offset, length)
					}, WorkDir: WorkDir, Response: Response,
				}
			}

			// block waiting for all responses to come in
			log.Printf("Waiting for completion of %d blocks", len(blockIndices))
			for _, response := range Responses {
				value, ok := <-response
				if ok {
					log.Printf("Ok")
				}
				log.Printf("Got response: %s", value)
			}
			log.Printf("Received completion for completion of %d blocks", len(blockIndices))
		}
		return requestCallback
	}

	var _requestDirEntries func(dirInode INode)

	var makeRequestDirEntries func(dirPath string) func(dirInode INode)

	makeRequestDirEntries = func(dirPath string) func(dirInode INode) {
		_requestDirEntries = func(dirInode INode) {
			Response := make(chan error)

			transferServiceQueue <- &GetDirRequest{
				GetDirListing: func(ctx context.Context) ([]RemoteFile, error) {
					if strings.HasPrefix(dirPath, "/") || strings.HasPrefix(dirPath, "./") || dirPath == "." {
						panic("bad dirPath")
					}
					return Remote.GetDirListing(ctx, dirPath)
				},
				DirINode: dirInode,
				MakeDirEntriesCallback: func(childName string) func(INode) {
					return makeRequestDirEntries(pathConcat(dirPath, childName))
				},
				MakeFileCallback: func(path string, etag string) RequestCallback {
					return makeRequestCallback(pathConcat(dirPath, path), etag)
				},
				Response: Response,
			}

			// wait for response before returning
			<-Response
		}

		return _requestDirEntries
	}

	fs.Root = fs.INodes.CreateLazyDir(UNALLOCATED_BLOCK_ID, &LazyDirectoryCallback{RequestDirEntries: makeRequestDirEntries("")})

	return fs, nil
}
