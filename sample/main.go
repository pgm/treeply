package main

import (
	"log"
	"os"

	"github.com/pgm/treeply"
)

func main() {
	log.Printf("starting...")
	workDir, err := os.MkdirTemp(os.TempDir(), "test")
	if err != nil {
		panic(err)
	}

	srcDir := os.Args[1]

	fs, err := treeply.NewFileService(&treeply.DirRemoteProvider{Root: srcDir}, workDir, 10000)
	if err != nil {
		panic(err)
	}

	socketName := "/tmp/treeply"

	log.Printf("create listener...")
	err = treeply.CreateListener(socketName, fs)
	if err != nil {
		panic(err)
	}
}
