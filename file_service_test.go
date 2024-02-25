package treeply

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func writeFile(path string, content string, count int) {
	parentDir := filepath.Dir(path)
	err := os.MkdirAll(parentDir, 0777)
	if err != nil {
		panic(err)
	}

	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	for i := 0; i < count; i++ {
		_, err = f.Write([]byte(content))
		if err != nil {
			panic(err)
		}
	}
	f.Close()
}

func TestWithDirRemote(t *testing.T) {
	assert.Equal(t, 1, 1)

	workDir, err := os.MkdirTemp(os.TempDir(), "test")
	if err != nil {
		panic(err)
	}

	tmpDir, err := os.MkdirTemp(os.TempDir(), "test")
	if err != nil {
		panic(err)
	}

	writeFile(tmpDir+"/f1", "f1", 10)
	writeFile(tmpDir+"/f2", "f2", 20)
	writeFile(tmpDir+"/d1/f1", "d1f1", 30)
	writeFile(tmpDir+"/d1/f2", "d1f2", 40)

	fs, err := NewFileService(&DirRemoteProvider{Root: tmpDir}, workDir, 10000)
	if err != nil {
		panic(err)
	}

	log.Printf("checkpoint1")
	dirEntries := fs.INodes.ReadDir(fs.Root)

	filenames := make([]string, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		filenames = append(filenames, dirEntry.Name)
	}
	sort.Strings(filenames)

	assert.Equal(t, []string{".", "..", "d1", "f1", "f2"}, filenames)

	log.Printf("checkpoint2")
	fileINode := fs.INodes.LookupInDir(fs.Root, "f1")
	assert.NotEqual(t, UNALLOCATED_BLOCK_ID, fileINode)

	log.Printf("checkpoint4")
	buffer := make([]byte, 4)
	n, err := fs.INodes.ReadFile(fileINode, 0, buffer)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "f1f1", string(buffer))

	log.Printf("checkpoint3")

	//    )  ._, mmeeoowwrr!
	//   (___)''
	//   / ,_,/
	//  /'"\ )\

}
