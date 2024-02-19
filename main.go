package treeply

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type INode uint64
type BlockID uint64

const UNALLOCATED_BLOCK_ID = 0

type BlockState struct {
	refCount int
}

type Blocks struct {
	lock        sync.Mutex
	blockStates map[BlockID]*BlockState
	nextBlockID BlockID
	freeBlockID []BlockID
	dir         string
	blockSize   uint64
}

type RequestCallback func(inode INode, blockIndices []int)

type INodeState struct {
	refCount        int
	length          uint64
	blocks          []BlockID
	requestCallback RequestCallback
}

type INodes struct {
	lock        sync.Mutex
	nextINode   INode
	freeINodes  []INode
	inodeStates map[INode]*INodeState

	blocks    *Blocks
	blockSize uint64

	workDir string
}

func NewINodes(workDir string, blockSize int) (*INodes, error) {
	inodes := &INodes{inodeStates: make(map[INode]*INodeState), workDir: workDir,
		blockSize: uint64(blockSize),
		blocks: &Blocks{blockStates: map[BlockID]*BlockState{},
			dir: workDir + "/blocks", blockSize: uint64(blockSize)}}

	err := os.MkdirAll(inodes.blocks.dir, 0777)
	if err != nil {
		return nil, err
	}

	return inodes, nil
}

func (i *INodes) Allocate(length uint64, requestCallback RequestCallback) INode {
	blocks := make([]BlockID, (length+i.blockSize-1)/i.blockSize)

	i.lock.Lock()
	var inode INode
	if len(i.freeINodes) > 0 {
		inode = i.freeINodes[len(i.freeINodes)-1]
		i.freeINodes = i.freeINodes[:len(i.freeINodes)-1]
	} else {
		i.nextINode += 1
		inode = i.nextINode
	}
	i.inodeStates[inode] = &INodeState{refCount: 1, requestCallback: requestCallback, length: length, blocks: blocks}
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

func (b *Blocks) UpdateRefCount(blockID BlockID, delta int) int {
	b.lock.Lock()

	state, ok := b.blockStates[blockID]
	if !ok {
		panic(fmt.Sprintf("accessed invalid block: %d", blockID))
	}

	state.refCount += delta
	refCount := state.refCount
	if refCount < 0 {
		panic("refcount < 0")
	} else if refCount == 0 {
		delete(b.blockStates, blockID)
		b.deleteFile(blockID)
	}

	b.lock.Unlock()

	return refCount
}

func (b *Blocks) Allocate(filename string) BlockID {
	fi, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}

	if fi.Size() > int64(b.blockSize) {
		panic("too big")
	}

	b.lock.Lock()

	var blockID BlockID
	if len(b.freeBlockID) > 0 {
		blockID = b.freeBlockID[len(b.freeBlockID)-1]
		b.freeBlockID = b.freeBlockID[:len(b.freeBlockID)-1]
	} else {
		b.nextBlockID += 1
		blockID = b.nextBlockID
	}
	b.blockStates[blockID] = &BlockState{refCount: 1}
	b.lock.Unlock()

	destName := b.getFilename(blockID)
	log.Printf("Renaming %s -> %s", filename, destName)
	err = os.Rename(filename, destName)
	if err != nil {
		panic("rename failed")
	}

	return blockID
}

func (b *Blocks) deleteFile(blockID BlockID) {
	filename := b.getFilename(blockID)
	err := os.Remove(filename)
	if err != nil {
		log.Fatalf("Could not delete %s: %s", filename, err)
	}
	log.Printf("Deleted %s", filename)
}

////////////////////

type BlockRange struct {
	blockID BlockID
	offset  uint32
	length  uint32
}

func (inodes *INodes) RequestMissingBlocks(inode INode, blockIndices []int) {
	inodes.lock.Lock()
	state := inodes.inodeStates[inode]
	requestCallback := state.requestCallback
	inodes.lock.Unlock()
	requestCallback(inode, blockIndices)
}

// edge cases: Read longer then file
func (inodes *INodes) Read(inode INode, offset uint64, buffer []byte) (int, error) {
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
	for _, blockID := range blockIDs {
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

func (b *Blocks) getFilename(blockID BlockID) string {
	return fmt.Sprintf("%s/%d", b.dir, blockID)
}

func (b *Blocks) ReadBlock(blockID BlockID, startOffsetWithinBlock int64, buffer []byte) (int, error) {
	filename := b.getFilename(blockID)
	f, err := os.Open(filename)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	log.Printf("ReadAt filename=%s, offset=%d, len=%d", filename, startOffsetWithinBlock, len(buffer))
	n, err := f.ReadAt(buffer, startOffsetWithinBlock)
	log.Printf("returned %d, %s", n, err)

	return n, err
}
