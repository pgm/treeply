package treeply

import (
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type ReadParams struct {
	offset int
	length int
}

func SkipTestLotsOfReadersFuzz(t *testing.T) {
	sourceLength := 10000
	maxRead := 101
	readThreads := 20
	blockSize := 23

	log.Printf("start")

	workDir := os.TempDir() + "/treeplytest"
	err := os.MkdirAll(workDir, 0777)
	if err != nil {
		panic(err)
	}

	inodes, err := NewINodes(workDir, blockSize)
	if err != nil {
		panic(err)
	}

	log.Printf("Creating source buffer")

	sourceBytes := make([]byte, sourceLength)
	for i := 0; i < sourceLength; i++ {
		sourceBytes[i] = byte(rand.Intn(256))
	}

	requestCallback := func(inode INode, blockIndices []int) {
		for _, index := range blockIndices {
			log.Printf("Request callback inode=%d, blockIndex=%d", inode, index)

			buffer := make([]byte, inodes.blockSize)
			blockStart := int(inodes.blockSize) * index
			blockLen := int(inodes.blockSize)
			if sourceLength-blockStart < blockLen {
				blockLen = sourceLength - blockStart
			}
			for i := 0; i < blockLen; i++ {
				buffer[i] = sourceBytes[i+blockStart]
			}

			// create temp file
			f, err := os.CreateTemp(inodes.workDir, "block")
			if err != nil {
				panic(err)
			}
			f.Write(buffer)
			f.Close()

			// assocate a block ID with that file
			blockID := inodes.blocks.Allocate(f.Name())
			inodes.SetBlock(inode, index, blockID)
		}
	}

	sampleInode := inodes.CreateLazyFile(int64(sourceLength), requestCallback)

	var readyToStart sync.WaitGroup
	var finished sync.WaitGroup
	readyToStart.Add(1)

	for readThreadI := 0; readThreadI < readThreads; readThreadI++ {
		finished.Add(1)

		log.Printf("Creating reader %d", readThreadI)

		// make a random pattern of reads
		readOff := make([]ReadParams, 0, 100)
		dest := 0
		for dest < sourceLength {
			length := rand.Intn(maxRead-1) + 1
			if dest+length > sourceLength {
				length = sourceLength - dest
			}
			readOff = append(readOff, ReadParams{offset: dest, length: length})
			dest += length
		}

		rand.Shuffle(len(readOff), func(i int, j int) {
			t := readOff[i]
			readOff[i] = readOff[j]
			readOff[j] = t
		})

		// allocate dest buffer
		destBuffer := make([]byte, sourceLength)

		go (func() {
			// wait until ready
			log.Printf("Reader %d waiting", readThreadI)
			readyToStart.Wait()

			// perform all reads
			for _, readParams := range readOff {
				log.Printf("read %d %d", readParams.offset, readParams.length)
				n, err := inodes.ReadFile(sampleInode, int64(readParams.offset), destBuffer[readParams.offset:readParams.offset+readParams.length])
				if err != nil {
					panic(err)
				}
				if n != readParams.length {
					panic(err)
				}
			}

			// verify contents
			for i := 0; i < sourceLength; i++ {
				if destBuffer[i] != sourceBytes[i] {
					log.Fatalf("dest[%d] = %d, source[%d] = %d", i, destBuffer[i], i, sourceBytes[i])
				}
			}

			finished.Done()
		})()
	}

	log.Printf("Setup complete")
	readyToStart.Done()
	finished.Wait()
	log.Printf("All complete")
}

func SkipTestReadFromLazyDir(t *testing.T) {
	workDir := os.TempDir()
	inodes, err := NewINodes(workDir, 3)
	if err != nil {
		panic(err)
	}

	requestBlocks := func(inode INode, blockIndices []int) {
		for _, index := range blockIndices {
			log.Printf("Request callback inode=%d, blockIndex=%d", inode, index)

			// create temp file
			f, err := os.CreateTemp(inodes.workDir, "block")
			if err != nil {
				panic(err)
			}
			buffer := make([]byte, inodes.blockSize)
			for i := 0; i < int(inodes.blockSize); i++ {
				buffer[i] = byte(index*int(inodes.blockSize) + i)
			}
			f.Write(buffer)
			f.Close()

			// assocate a block ID with that file
			blockID := inodes.blocks.Allocate(f.Name())
			inodes.SetBlock(inode, index, blockID)
		}
	}

	var requestDir func(inode INode)
	requestDir = func(inode INode) {
		childFile := inodes.CreateLazyFile(10, requestBlocks)
		childDir := inodes.CreateLazyDir(inode, &LazyDirectoryCallback{RequestDirEntries: requestDir})
		inodes.SetDirEntries(inode, []DirEntry{{Name: "file", INode: childFile}, {Name: "dir", INode: childDir}})
	}
	sampleInode := inodes.CreateLazyDir(0, &LazyDirectoryCallback{RequestDirEntries: requestDir})

	dirEntries := inodes.ReadDir(sampleInode)
	if len(dirEntries) != 4 {
		t.Errorf("expected 4")
	}

	assert.Equal(t, dirEntries[0].Name, ".")
	assert.Equal(t, dirEntries[0].INode, sampleInode)
	assert.Equal(t, dirEntries[1].Name, "..")
	assert.Equal(t, dirEntries[1].INode, sampleInode)
	assert.Equal(t, dirEntries[2].Name, "file")
	assert.Equal(t, dirEntries[3].Name, "dir")

	fileINode := dirEntries[2].INode

	buffer := make([]byte, 2)
	n, err := inodes.ReadFile(fileINode, 0, buffer)
	if n != 2 {
		t.Errorf("n=%d", n)
	}

	if err != nil {
		t.Errorf("err=%s", err)
	}

	if buffer[0] != 0 || buffer[1] != 1 {
		t.Errorf("buffer={%d, %d, ..}", buffer[0], buffer[1])
	}

	// // read the firs 10 bytes (span 3 pages, and one partial page)
	// buffer := make([]byte, 10)
	// n, err := inodes.ReadFile(sampleInode, 0, buffer)
	// if n != 10 {
	// 	t.Errorf("n=%d", n)
	// }

	// if err != nil {
	// 	t.Errorf("err=%s", err)
	// }

	// if buffer[0] != 0 || buffer[1] != 1 {
	// 	t.Errorf("buffer={%d, %d, ..}", buffer[0], buffer[1])
	// }

	// // try reading the last 2 bytes
	// buffer = make([]byte, 2)
	// n, err = inodes.ReadFile(sampleInode, 9, buffer)
	// if n != 2 {
	// 	t.Errorf("n=%d", n)
	// }

	// if err != nil {
	// 	t.Errorf("err=%s", err)
	// }

	// if buffer[0] != 9 || buffer[1] != 10 {
	// 	t.Errorf("buffer={%d, %d, ..}", buffer[0], buffer[1])
	// }

}

// if the file is lazy, check for unallocated block ids. If any, request those blocks populated and then try again after ack.
func TestReadFromLazyINode(t *testing.T) {
	workDir := os.TempDir()
	inodes, err := NewINodes(workDir, 3)
	if err != nil {
		panic(err)
	}

	requestCallback := func(inode INode, blockIndices []int) {
		for _, index := range blockIndices {
			log.Printf("Request callback inode=%d, blockIndex=%d", inode, index)

			// create temp file
			f, err := os.CreateTemp(inodes.workDir, "block")
			if err != nil {
				panic(err)
			}
			buffer := make([]byte, inodes.blockSize)
			for i := 0; i < int(inodes.blockSize); i++ {
				buffer[i] = byte(index*int(inodes.blockSize) + i)
			}
			f.Write(buffer)
			f.Close()

			// assocate a block ID with that file
			blockID := inodes.blocks.Allocate(f.Name())
			inodes.SetBlock(inode, index, blockID)
		}
	}
	sampleInode := inodes.CreateLazyFile(11, requestCallback)

	// read the firs 10 bytes (span 3 pages, and one partial page)
	buffer := make([]byte, 10)
	n, err := inodes.ReadFile(sampleInode, 0, buffer)
	if n != 10 {
		t.Errorf("n=%d", n)
	}

	if err != nil {
		t.Errorf("err=%s", err)
	}

	if buffer[0] != 0 || buffer[1] != 1 {
		t.Errorf("buffer={%d, %d, ..}", buffer[0], buffer[1])
	}

	// try reading the last 2 bytes
	buffer = make([]byte, 2)
	n, err = inodes.ReadFile(sampleInode, 9, buffer)
	if n != 2 {
		t.Errorf("n=%d", n)
	}

	if err != nil {
		t.Errorf("err=%s", err)
	}

	if buffer[0] != 9 || buffer[1] != 10 {
		t.Errorf("buffer={%d, %d, ..}", buffer[0], buffer[1])
	}

	// should release the inode and release all associated blocks
	inodes.UpdateRefCount(sampleInode, -1)

	if len(inodes.inodeStates) != 0 {
		t.Errorf("inode not freed")
	}

	if len(inodes.blocks.blockStates) != 0 {
		t.Errorf("blocks not freed")
	}
}

// func TestReadFromAutomountDir(t *testing.T) {
// 	workDir := os.TempDir()
// 	inodes, err := NewINodes(workDir, 3)
// 	if err != nil {
// 		panic(err)
// 	}

// 	requestBlocks := func(inode INode, blockIndices []int) {
// 		panic("not impl")
// 	}

// 	requestDirEntry := func(inode INode, name string) {
// 		if name == "missing" {
// 			log.Printf("populating missing")
// 			inodes.SetDirEntry(inode, name, UNALLOCATED_BLOCK_ID)
// 		} else {
// 			childFile := inodes.CreateLazyFile(10, requestBlocks)
// 			log.Printf("populating %s -> %d", name, childFile)
// 			inodes.SetDirEntry(inode, name, childFile)
// 		}
// 	}
// 	sampleInode := inodes.CreateLazyDir(0, &LazyDirectoryCallback{RequestDirEntry: requestDirEntry})

// 	dirEntries := inodes.ReadDir(sampleInode)
// 	assert.Equal(t, 2, len(dirEntries))

// 	inodes.LookupInDir(sampleInode, "x")

// 	dirEntries = inodes.ReadDir(sampleInode)
// 	assert.Equal(t, 3, len(dirEntries))

// 	inodes.LookupInDir(sampleInode, "y")

// 	dirEntries = inodes.ReadDir(sampleInode)
// 	assert.Equal(t, 4, len(dirEntries))

// 	inodes.LookupInDir(sampleInode, "missing")

// 	dirEntries = inodes.ReadDir(sampleInode)
// 	assert.Equal(t, 4, len(dirEntries))

// }
