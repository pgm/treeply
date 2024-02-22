package treeply

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

func NewINodes(workDir string, blockSize int) (*INodes, error) {
	inodes := &INodes{inodeStates: make(map[INode]*INodeState), workDir: workDir,
		blockSize: uint64(blockSize),
		blocks: &Blocks{nextBlockID: 1, blockStates: map[BlockID]*BlockState{},
			dir: workDir + "/blocks", blockSize: uint64(blockSize)}}

	err := os.MkdirAll(inodes.blocks.dir, 0777)
	if err != nil {
		return nil, err
	}

	return inodes, nil
}

func (i *INodes) getNextINode() INode {
	var inode INode
	if len(i.freeINodes) > 0 {
		inode = i.freeINodes[len(i.freeINodes)-1]
		i.freeINodes = i.freeINodes[:len(i.freeINodes)-1]
	} else {
		i.nextINode += 1
		inode = i.nextINode
	}
	return inode
}

func (i *INodes) CreateLazyDir(parentINode INode, callback *LazyDirectoryCallback) INode {
	i.lock.Lock()
	inode := i.getNextINode()
	if parentINode == 0 {
		// special case: Root is it's own parent
		parentINode = inode
	}
	i.inodeStates[inode] = &INodeState{
		refCount:              1,
		lazyDirectoryCallback: callback,
		isDir:                 true,
		dirEntries:            NewDirEntries(inode, parentINode)}
	i.lock.Unlock()
	return inode
}

func (i *INodes) CreateLazyFile(length uint64, requestCallback RequestCallback) INode {
	blocks := make([]BlockID, (length+i.blockSize-1)/i.blockSize)

	i.lock.Lock()
	inode := i.getNextINode()
	i.inodeStates[inode] = &INodeState{
		refCount:        1,
		requestCallback: requestCallback,
		length:          length,
		blocks:          blocks,
		isDir:           false}
	i.lock.Unlock()
	return inode
}

func (i *INodes) UpdateRefCount(inode INode, delta int) int {
	i.lock.Lock()
	inodeState, ok := i.inodeStates[inode]
	if !ok {
		panic("no such inode")
	}
	inodeState.refCount += delta
	refCount := inodeState.refCount
	if refCount < 0 {
		panic("refcount < 0")
	} else if refCount == 0 {
		// free inode
		for _, blockID := range inodeState.blocks {
			i.blocks.UpdateRefCount(blockID, -1)
		}
		delete(i.inodeStates, inode)
	}
	i.lock.Unlock()

	return refCount
}

func (in *INodes) SetDirEntry(inode INode, name string, _inode INode) {
	in.lock.Lock()

	inodeState, ok := in.inodeStates[inode]
	if !ok {
		panic("no such inode")
	}

	inodeState.dirEntries.SetEntry(name, _inode)

	in.lock.Unlock()
}

func (in *INodes) SetDirEntries(inode INode, dirEntries []DirEntry) {
	in.lock.Lock()

	inodeState, ok := in.inodeStates[inode]
	if !ok {
		panic("no such inode")
	}

	inodeState.dirEntries.Set(dirEntries)
	inodeState.isDirPopulated = true

	in.lock.Unlock()
}

func (in *INodes) SetBlock(inode INode, index int, blockID BlockID) {
	in.lock.Lock()

	inodeState, ok := in.inodeStates[inode]
	if !ok {
		panic("no such inode")
	}

	// add extra blocks if index is past the end, suggesting the file has gotten longer
	for len(inodeState.blocks) <= index {
		inodeState.blocks = append(inodeState.blocks, UNALLOCATED_BLOCK_ID)
	}

	inodeState.blocks[index] = blockID

	in.lock.Unlock()
}

func (in *INodes) GetBlockIDs(inode INode, startIndex uint64, count uint64) []BlockID {
	fmt.Printf("count=%d\n", count)
	result := make([]BlockID, count)
	in.lock.Lock()

	inodeState, ok := in.inodeStates[inode]
	if !ok {
		panic("no such inode")
	}

	for i := uint64(0); i < count; i += 1 {
		blockID := inodeState.blocks[startIndex+i]
		result[i] = blockID
		if blockID != UNALLOCATED_BLOCK_ID {
			in.blocks.UpdateRefCount(blockID, 1)
		}
	}

	in.lock.Unlock()
	return result
}

func (inodes *INodes) RequestMissingBlocks(inode INode, blockIndices []int) {
	inodes.lock.Lock()
	state := inodes.inodeStates[inode]
	requestCallback := state.requestCallback
	inodes.lock.Unlock()
	requestCallback(inode, blockIndices)
}

var INVALID_INODE = errors.New("Invalid INode")
var IS_NOT_DIR = errors.New("INode is not a directory")

func (inodes *INodes) LookupInDirWithErr(dirINode INode, name string) (INode, error) {
	inodes.lock.Lock()

	inodeState, ok := inodes.inodeStates[dirINode]
	if !ok {
		return 0, INVALID_INODE
	}

	if !inodeState.isDir {
		return 0, IS_NOT_DIR
	}

	if !inodeState.dirEntries.IsPopulated(name) {
		inodes.lock.Unlock()
		inodeState.lazyDirectoryCallback.RequestDirEntry(dirINode, name)
		inodes.lock.Lock()
	}

	if !inodeState.dirEntries.IsPopulated(name) {
		log.Fatalf("callback did not populate %s", name)
	}

	result := inodeState.dirEntries.Lookup(name)

	inodes.lock.Unlock()
	return result, nil
}

func (inodes *INodes) LookupInDir(dirINode INode, name string) INode {
	inode, err := inodes.LookupInDirWithErr(dirINode, name)
	if err != nil {
		panic(err)
	}
	return inode
}

func (inodes *INodes) ReadDir(inode INode) []DirEntry {
	inodes.lock.Lock()

	inodeState, ok := inodes.inodeStates[inode]
	if !ok {
		panic("no such inode")
	}

	if !inodeState.isDir {
		panic("not dir")
	}

	// if we're a directory but not populated, use callback to request it be populated
	if !inodeState.isDirPopulated && inodeState.lazyDirectoryCallback.RequestDirEntries != nil {
		inodes.lock.Unlock()
		inodeState.lazyDirectoryCallback.RequestDirEntries(inode)
		inodes.lock.Lock()
		if !inodeState.isDirPopulated {
			panic("requestCallback did not populate dir")
		}
	}

	result := inodeState.dirEntries.Get()

	inodes.lock.Unlock()
	return result
}

// edge cases: ReadFile longer then file
func (inodes *INodes) ReadFile(inode INode, offset uint64, buffer []byte) (int, error) {
	startIndex := offset / inodes.blockSize
	startOffsetWithinBlock := offset % inodes.blockSize
	endIndex := (offset + uint64(len(buffer)) + inodes.blockSize - 1) / inodes.blockSize
	blockCount := endIndex - startIndex

	blockIDs := inodes.GetBlockIDs(inode, startIndex, blockCount)

	// iterate through block IDs, checking to see if any blocks are unallocated
	missingBlockIDs := make([]int, 0, len(blockIDs))
	for i, blockID := range blockIDs {
		if blockID == UNALLOCATED_BLOCK_ID {
			missingBlockIDs = append(missingBlockIDs, int(startIndex)+i)
		}
	}

	if len(missingBlockIDs) > 0 {
		inodes.RequestMissingBlocks(inode, missingBlockIDs)
		// after the above has completed, we should be able to get the final version of the block IDs
		blockIDs = inodes.GetBlockIDs(inode, startIndex, blockCount)
	}

	defer (func() {
		// now that we're done, release these blocks
		for _, blockID := range blockIDs {
			if blockID != UNALLOCATED_BLOCK_ID {
				inodes.blocks.UpdateRefCount(blockID, -1)
			}
		}
	})()

	// do the actual read
	destOffset := 0
	for blockIndex, blockID := range blockIDs {
		if blockID == UNALLOCATED_BLOCK_ID {
			log.Fatalf("Block index %d was still not populated", blockIndex)
		}
		readLength := len(buffer) - destOffset
		blockLength, err := inodes.blocks.ReadBlock(blockID, int64(startOffsetWithinBlock), buffer[destOffset:destOffset+readLength])
		destOffset += blockLength

		if err != nil && err != io.EOF {
			return destOffset, err
		}

		startOffsetWithinBlock = 0
	}

	return int(destOffset), nil
}
