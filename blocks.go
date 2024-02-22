package treeply

import (
	"fmt"
	"log"
	"os"
)

func (b *Blocks) deleteFile(blockID BlockID) {
	filename := b.getFilename(blockID)
	err := os.Remove(filename)
	if err != nil {
		log.Fatalf("Could not delete %s: %s", filename, err)
	}
	log.Printf("Deleted %s", filename)
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
