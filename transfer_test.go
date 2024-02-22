package treeply

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransfer(t *testing.T) {
	var inode INode = 1
	blockSize := 20
	blockIndex := 0
	tempDir := os.TempDir()
	completions := make(chan *BlockCompletion)
	source := make([]byte, 50)
	for i := 0; i < len(source); i++ {
		source[i] = byte(i)
	}
	reader := bytes.NewBuffer(source)
	readChunkSize := 13

	go (func() {
		Transfer(context.Background(),
			inode, blockSize, blockIndex, tempDir,
			completions, reader, readChunkSize)
		close(completions)
	})()

	// should get three completions
	c := []*BlockCompletion{<-completions, <-completions,
		<-completions}

	assert.Equal(t, 0, c[0].BlockIndex)
	fi, err := os.Stat(c[0].Filename)
	assert.Equal(t, nil, err)
	assert.Equal(t, 20, int(fi.Size()))

	assert.Equal(t, 1, c[1].BlockIndex)
	fi, err = os.Stat(c[1].Filename)
	assert.Equal(t, nil, err)
	assert.Equal(t, 20, int(fi.Size()))

	assert.Equal(t, 2, c[2].BlockIndex)
	fi, err = os.Stat(c[2].Filename)
	assert.Equal(t, nil, err)
	assert.Equal(t, 10, int(fi.Size()))

	// make sure we only get these three
	_, remaining := <-completions
	assert.False(t, remaining)
}
