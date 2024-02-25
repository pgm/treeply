package treeply

import (
	"log"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileClientOperations(t *testing.T) {

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

	client := NewFileClient(fs)

	// list dir should work
	// list a file should fail
	// list an invalid file should fail
	checkListDir(client, t)

	// opening a file should work
	//	resp, err := client.Open(&OpenReq{})
}

func checkOpen(client *FileClient, t *testing.T) {
	// opening a dir should fail
	resp, err := client.Open(&OpenReq{Path: "d1"})
	assert.Equal(t, nil, resp)
	assert.Equal(t, IS_DIR, err)

	// opening a missing file should fail
	resp, err = client.Open(&OpenReq{Path: "f3"})
	assert.Equal(t, nil, resp)
	assert.Equal(t, INVALID_NAME, err)

	// opening a file should be fine
	resp, err = client.Open(&OpenReq{Path: "f1"})
	assert.Equal(t, nil, err)
	fd := resp.FD

	// and now we can read
	checkRead(client, t, fd)

	checkClose(client, t, fd)
}

func checkClose(client *FileClient, t *testing.T, fd int) {
	resp, err := client.Close(&CloseReq{FD: fd})
	assert.Equal(t, nil, err)
	assert.Equal(t, &CloseResp{}, resp)
}

func checkRead(client *FileClient, t *testing.T, fd int) {
	// read a little
	resp, err := client.Read(&ReadReq{FD: fd, Length: 3})
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{'f', '1', 'f'}, resp.Data)

	// and then a little more
	resp, err = client.Read(&ReadReq{FD: fd, Length: 2})
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{'1', 'f'}, resp.Data)

	// and we've read 5 bytes so far. Now try to read past the end and
	// confirm we only get 15 more bytes
	resp, err = client.Read(&ReadReq{FD: fd, Length: 1000})
	assert.Equal(t, nil, err)
	assert.Equal(t, 15, len(resp.Data))
}

func checkListDir(client *FileClient, t *testing.T) {
	log.Printf("checkListDir1")
	resp, err := client.ListDir(&ListDirReq{Path: "."})
	assert.Equal(t, nil, err)
	names := make([]string, 0, len(resp.Entries))
	for _, entry := range resp.Entries {
		names = append(names, entry.Name)
	}
	sort.Strings(names)
	assert.Equal(t, []string{".", "..", "d1", "f1", "f2"}, names)

	log.Printf("checkListDir2")
	resp, err = client.ListDir(&ListDirReq{Path: "d1"})
	assert.Equal(t, nil, err)
	names = make([]string, 0, len(resp.Entries))
	for _, entry := range resp.Entries {
		names = append(names, entry.Name)
	}
	sort.Strings(names)
	assert.Equal(t, []string{".", "..", "f1", "f2"}, names)

	log.Printf("checkListDir3")
	resp, err = client.ListDir(&ListDirReq{Path: "f1"})
	log.Printf("resp=%v err=%s", resp, err)
	assert.Nil(t, resp)
	assert.Equal(t, IS_NOT_DIR, err)

	log.Printf("checkListDir4")
	resp, err = client.ListDir(&ListDirReq{Path: "f3"})
	assert.Nil(t, resp)
	assert.Equal(t, INVALID_NAME, err)
}
