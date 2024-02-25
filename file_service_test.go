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

	getDirAsStrs := func(dirINode INode) []string {
		dirEntries := fs.INodes.ReadDir(dirINode)

		filenames := make([]string, 0, len(dirEntries))
		for _, dirEntry := range dirEntries {
			filenames = append(filenames, dirEntry.Name)
		}
		sort.Strings(filenames)
		return filenames
	}

	filenames := getDirAsStrs(fs.Root)
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

	// now update a directory by adding a file
	writeFile(tmpDir+"/f3", "f3", 20)
	// we can't see it because the old dir is cached
	filenames = getDirAsStrs(fs.Root)
	assert.Equal(t, []string{".", "..", "d1", "f1", "f2"}, filenames)

	// however if we tell it to forget, we should be able to see it.
	fs.Forget("")
	filenames = getDirAsStrs(fs.Root)
	assert.Equal(t, []string{".", "..", "d1", "f1", "f2", "f3"}, filenames)

	checkReadMutatedFails(t, fs, tmpDir)

	checkFileDisappeared(t, fs, tmpDir)
	//    )  ._, mmeeoowwrr!
	//   (___)''
	//   / ,_,/
	//  /'"\ )\

}

func checkReadMutatedFails(t *testing.T, fs *FileService, tmpDir string) {
	log.Printf("checkpoint4")
	buffer := make([]byte, 4)
	f1INode, err := fs.GetINodeForPath("f1")
	assert.Equal(t, nil, err)

	n, err := fs.INodes.ReadFile(f1INode, 0, buffer)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, n)

	// now mutate that file
	writeFile(tmpDir+"/f1", "xyz", 10)
	n, err = fs.INodes.ReadFile(f1INode, 0, buffer)
	assert.Equal(t, 0, n)
	assert.Equal(t, FILE_CHANGED, err)
}

func checkFileDisappeared(t *testing.T, fs *FileService, tmpDir string) {
	log.Printf("checkpoint4")
	buffer := make([]byte, 4)
	f1INode, err := fs.GetINodeForPath("f3")
	assert.Equal(t, nil, err)

	// now delete that file
	err = os.Remove(tmpDir + "/f3")
	assert.Equal(t, nil, err)

	n, err := fs.INodes.ReadFile(f1INode, 0, buffer)
	assert.Equal(t, 0, n)
	assert.Equal(t, FILE_CHANGED, err)
}
