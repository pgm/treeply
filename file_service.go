package treeply

import (
	"context"
	"log"
)

type FileService struct {
	Remote RemoteProvider
	INodes *INodes
	Root   INode
}

func NewFileService(Remote RemoteProvider, WorkDir string, BlockSize int) (*FileService, error) {
	inodes, err := NewINodes(WorkDir, int(BlockSize))
	if err != nil {
		return nil, err
	}

	fs := &FileService{Remote: Remote, INodes: inodes}

	ctx := context.Background()

	// TODO: These implementations cause a fetch to always happen. This means
	// that there's race conditions that can happen (ie: two threads ask for
	// the same block) where we could make a single transfer as opposed to two.
	// So, functionally, correct, but may be inefficient. Probably could add
	// logging in SetBlocks to warn every time it sees we're replacing an
	// existing block with a new one.

	makeRequestCallback := func(path string, etag string) RequestCallback {
		requestCallback := func(inode INode, blockIndices []int) {
			completions := make(chan *BlockCompletion)

			go (func() {
				for _, blockIndex := range blockIndices {
					reader, err := Remote.GetReader(ctx, path, etag, int64(blockIndex)*int64(BlockSize), int64(BlockSize))
					if err != nil {
						log.Printf("Error in requestDirEntries: %s", err)
						return
					}
					readChunkSize := 1000
					Transfer(ctx, inode, BlockSize, blockIndex, WorkDir, completions, reader, readChunkSize)
				}
				close(completions)
			})()

			for completion := range completions {
				blockID := inodes.blocks.Allocate(completion.Filename)
				log.Printf("mapping %s to block %d", completion.Filename, blockID)
				inodes.SetBlock(completion.INode, completion.BlockIndex, blockID)
			}

		}
		return requestCallback
	}

	var _requestDirEntries func(dirInode INode)

	var makeRequestDirEntries func(dirPath string) func(dirInode INode)

	makeRequestDirEntries = func(dirPath string) func(dirInode INode) {
		_requestDirEntries = func(dirInode INode) {
			files, err := Remote.GetDirListing(ctx, "")
			if err != nil {
				log.Printf("Error in requestDirEntries: %s", err)
				return
			}

			dirEntries := make([]DirEntry, 0, len(files))
			for _, file := range files {
				var inode INode
				if file.IsDir {
					inode = fs.INodes.CreateLazyDir(dirInode, &LazyDirectoryCallback{RequestDirEntries: makeRequestDirEntries(dirPath + "/" + file.Name)})
				} else {
					inode = fs.INodes.CreateLazyFile(uint64(file.Size), makeRequestCallback(dirPath+"/"+file.Name, file.ETag))
				}
				dirEntries = append(dirEntries, DirEntry{Name: file.Name, INode: inode})
			}

			fs.INodes.SetDirEntries(dirInode, dirEntries)
		}

		// requestDirEntries := func(dirInode INode) {
		// 	go _requestDirEntries(dirInode)
		// }

		return _requestDirEntries
	}

	fs.Root = fs.INodes.CreateLazyDir(UNALLOCATED_BLOCK_ID, &LazyDirectoryCallback{RequestDirEntries: makeRequestDirEntries("")})

	return fs, nil
}
