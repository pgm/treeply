package treeply

import (
	"context"
	"io"
	"log"
	"os"
)

const ReadChunkSize = 1024 * 1024

type TransferServiceStatus struct {
	BlocksRequested         int
	ThreadsWaitingForBlocks int
	DirsRequested           int
	ThreadsWaitingForDirs   int
}

type INodeBlock struct {
	INode      INode
	BlockIndex int
}

type DiagnosticRequest struct {
	Response chan *TransferServiceStatus
}

type BlockRequest struct {
	Block     INodeBlock
	BlockSize int64
	GetReader func(context.Context) (io.Reader, error)
	WorkDir   string
	Response  chan error
}

type BlockCompletion struct {
	Block    INodeBlock
	Filename string
}

type InProgressState struct {
	Waiting []chan error
}

type GetDirRequest struct {
	GetDirListing          func(context.Context) ([]RemoteFile, error)
	DirINode               INode
	MakeDirEntriesCallback func(name string) func(inode INode)
	MakeFileCallback       func(name string, etag string) RequestCallback
	Response               chan error
}

type GetDirCompletion struct {
	DirEntries []DirEntry
	DirINode   INode
}

type DirProgressState struct {
	Waiting []chan error
}

func TransferService(queue chan interface{}, INodes *INodes) {
	blockRequests := make(map[INodeBlock]*InProgressState)
	dirRequests := make(map[INode]*DirProgressState)

	for _request := range queue {
		switch request := _request.(type) {
		case *BlockRequest:
			doBlockRequest(blockRequests, request, INodes, queue)
		case *BlockCompletion:
			doBlockCompletion(blockRequests, INodes, request)
		case *GetDirRequest:
			doGetDir(dirRequests, INodes, request, queue)
		case *GetDirCompletion:
			doGetDirCompletion(dirRequests, INodes, request)
		case *DiagnosticRequest:
			doDiagnosticRequest(blockRequests, dirRequests, request)
		default:
			panic("unknown msg")
		}

	}
}

func doBlockRequest(blockState map[INodeBlock]*InProgressState, request *BlockRequest, inodes *INodes, mailbox chan interface{}) {
	log.Printf("Received block request: %d:%d", request.Block.INode, request.Block.BlockIndex)
	state, ok := blockState[request.Block]
	if ok {
		// if this block is already in progress, so just add this request to the waiting list
		state.Waiting = append(state.Waiting, request.Response)
		return
	} else {
		// if we don't have as a block which is in progress, check to see if maybe
		// it's already been populated while this request has been waiting in the
		// queue.
		if inodes.IsBlockPopulated(request.Block.INode, request.Block.BlockIndex) {
			log.Printf("Block %d:%d is already populated", request.Block.INode, request.Block.BlockIndex)
			close(request.Response)
			return
		}
	}

	ctx := context.Background()
	// start a new transfer
	blockState[request.Block] = &InProgressState{Waiting: []chan error{request.Response}}
	log.Printf("starting transfer for %d:%d", request.Block.INode, request.Block.BlockIndex)
	go startTransfer(ctx, mailbox, request.WorkDir, request.Block.INode, request.Block.BlockIndex,
		inodes.blockSize, request.GetReader)
}

func doBlockCompletion(blockState map[INodeBlock]*InProgressState, inodes *INodes, completion *BlockCompletion) {
	log.Printf("completed transfer for %d:%d", completion.Block.INode, completion.Block.BlockIndex)

	blockID := inodes.blocks.Allocate(completion.Filename)
	log.Printf("mapping %s to block %d", completion.Filename, blockID)
	inodes.SetBlock(completion.Block.INode, completion.Block.BlockIndex, blockID)
	log.Printf("setblock called for %d:%d", completion.Block.INode, completion.Block.BlockIndex)

	state, ok := blockState[completion.Block]
	if ok {
		log.Printf("waking %d threads", len(state.Waiting))
		for _, waiting := range state.Waiting {
			close(waiting)
		}
	} else {
		log.Printf("Warning: Got block completion of block not requested")
	}
}

func startTransfer(ctx context.Context, completions chan interface{}, WorkDir string, inode INode, blockIndex int, BlockSize int64, GetReader func(context.Context) (io.Reader, error)) {
	reader, err := GetReader(ctx) // Remote.GetReader(ctx, path, etag, int64(blockIndex)*BlockSize, BlockSize)
	if err != nil {
		log.Printf("Error in requestDirEntries: %s", err)
		return
	}
	err = Transfer(ctx, inode, BlockSize, blockIndex, WorkDir, completions, reader, ReadChunkSize)
	log.Printf("err=%s", err)
}

func Transfer(ctx context.Context, inode INode, blockSize int64, blockIndex int, tempDir string, completions chan interface{}, reader io.Reader, readChunkSize int) error {
	if blockSize == 0 {
		panic("blocksize==0")
	}
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
			completions <- &BlockCompletion{Block: INodeBlock{INode: inode, BlockIndex: blockIndex}, Filename: file.Name()}
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
				bytesInBlockRemaining = int(blockSize)
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

func doGetDirCompletion(dirRequests map[INode]*DirProgressState, inodes *INodes, request *GetDirCompletion) {
	inodes.SetDirEntries(request.DirINode, request.DirEntries)

	// notify any waiting that the request has completed
	existing, ok := dirRequests[request.DirINode]
	if !ok {
		panic("missing dir request")
	}
	for _, waiting := range existing.Waiting {
		close(waiting)
	}

	// remove from the list of in-progress requests
	delete(dirRequests, request.DirINode)
}

func doGetDir(dirRequests map[INode]*DirProgressState, inodes *INodes, request *GetDirRequest, mailbox chan interface{}) {

	existing, ok := dirRequests[request.DirINode]
	if ok {
		// if there's an existing request, simply add this to the list of channels to notify when the request is
		// complete
		existing.Waiting = append(existing.Waiting, request.Response)
		return
	}

	// double check that this dir isn't yet populated
	if inodes.IsDirPopulated(request.DirINode) {
		// if so, it must have gotten populated in parallel. Notify thread its done
		close(request.Response)
	}

	// We must create a new request to get the dir contents
	dirRequests[request.DirINode] = &DirProgressState{Waiting: []chan error{request.Response}}
	ctx := context.Background()
	go startGetDir(ctx, inodes, request, mailbox)

}

func startGetDir(ctx context.Context, inodes *INodes, request *GetDirRequest, mailbox chan interface{}) {
	files, err := request.GetDirListing(ctx)
	if err != nil {
		log.Printf("Error in requestDirEntries: %s", err)
		return
	}

	dirEntries := make([]DirEntry, 0, len(files))
	for _, file := range files {
		var inode INode
		if file.IsDir {
			inode = inodes.CreateLazyDir(request.DirINode, &LazyDirectoryCallback{RequestDirEntries: request.MakeDirEntriesCallback(file.Name)})
		} else {
			inode = inodes.CreateLazyFile(file.Size, request.MakeFileCallback(file.Name, file.ETag))
		}
		dirEntries = append(dirEntries, DirEntry{Name: file.Name, INode: inode})
	}

	mailbox <- &GetDirCompletion{DirINode: request.DirINode, DirEntries: dirEntries}
}

func doDiagnosticRequest(blockState map[INodeBlock]*InProgressState, dirRequests map[INode]*DirProgressState, request *DiagnosticRequest) {
	ThreadsWaitingForBlocks := 0
	for _, request := range blockState {
		ThreadsWaitingForBlocks += len(request.Waiting)
	}

	ThreadsWaitingForDirs := 0
	for _, request := range dirRequests {
		ThreadsWaitingForDirs += len(request.Waiting)
	}

	response := &TransferServiceStatus{
		BlocksRequested:         len(blockState),
		ThreadsWaitingForBlocks: ThreadsWaitingForBlocks,
		DirsRequested:           len(dirRequests),
		ThreadsWaitingForDirs:   ThreadsWaitingForDirs,
	}

	request.Response <- response
	close(request.Response)
}
