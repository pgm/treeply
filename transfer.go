package treeply

import (
	"context"
	"io"
	"log"
	"os"

	"cloud.google.com/go/storage"
)

type BlockCompletion struct {
	INode      INode
	Filename   string
	BlockIndex int
}

type GCSOperations struct {
	Client *storage.Client
}

func NewGCSOperations(ctx context.Context) *GCSOperations {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return &GCSOperations{Client: client}
}

// banana banana hedgehog

// /`-.__                                  /\
//  `. .  ~~--..__                   __..-' ,'
//    `.`-.._     ~~---...___...---~~  _,~,/
// 	   `-._ ~--..__             __..-~ _-~
// 	    	~~-..__ ~~--.....--~~   _.-~
// 		    	   ~~--...___...--~~
// /`-.__                                  /\
//  `. .  ~~--..__                   __..-' ,'
//    `.`-.._     ~~---...___...---~~  _,~,/
// 	   `-._ ~--..__             __..-~ _-~
// 	    	~~-..__ ~~--.....--~~   _.-~
// 		    	   ~~--...___...--~~
//
//   .::::::::..          ..::::::::.
//  :::::::::::::        :::::::::::::
// :::::::::::' .\      /. `:::::::::::
// `::::::::::_,__o    o__,_::::::::::'

const ReadChunkSize = 1024 * 1024

// bucket string, key string,
// rc, err := client.Bucket(bucket).Object(key).NewRangeReader(ctx, int64(blockSize)*int64(blockIndex), int64(blockCount)*int64(blockSize))
// if err != nil {
// 	log.Fatal(err)
// }

func Transfer(ctx context.Context, inode INode, blockSize int, blockIndex int, tempDir string, completions chan *BlockCompletion, reader io.Reader, readChunkSize int) error {
	if readerCloser, ok := reader.(io.Closer); ok {
		defer readerCloser.Close()
	}

	var file *os.File
	var bytesInBlockRemaining int

	finishCurrentFile := func() error {
		if file != nil {
			log.Printf("closing current file and sending completion for block index %d", blockIndex)
			err := file.Close()
			if err != nil {
				return err
			}
			completions <- &BlockCompletion{INode: inode, Filename: file.Name(), BlockIndex: blockIndex}
			blockIndex++
			file = nil
		}
		return nil
	}

	writeToTemp := func(buffer []byte) error {
		var err error
		offset := 0
		for offset < len(buffer) {
			if file == nil {
				// if we don't have a file open already, create a new temp file
				file, err = os.CreateTemp(tempDir, "block")
				if err != nil {
					return err
				}
				bytesInBlockRemaining = blockSize
				log.Printf("Created tmp file %s", file.Name())
			}

			writeLen := len(buffer) - offset
			if writeLen > bytesInBlockRemaining {
				writeLen = bytesInBlockRemaining
			}

			log.Printf("Writing %d bytes to %s", writeLen, file.Name())
			n, err := file.Write(buffer[offset : offset+writeLen])
			if err != nil {
				return err
			}
			if n != writeLen {
				panic("Did not complete write, but no error")
			}

			bytesInBlockRemaining -= writeLen
			offset += writeLen

			if bytesInBlockRemaining == 0 {
				err = finishCurrentFile()
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	buffer := make([]byte, readChunkSize)
	for {
		n, err := reader.Read(buffer)
		log.Printf("read completed n=%d, err=%s", n, err)
		writeToTemp(buffer[:n])
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	log.Printf("done reading")
	return finishCurrentFile()
}
