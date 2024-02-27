package treeply

import (
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

type LazyDirectoryCallback struct {
	RequestDirEntries func(inode INode)
	RequestDirEntry   func(inode INode, name string)
}

type INodeState struct {
	refCount              int
	length                int64
	isDir                 bool
	isDirPopulated        bool
	readFailed            error
	blocks                []BlockID
	dirEntries            *DirEntries
	requestCallback       RequestCallback
	lazyDirectoryCallback *LazyDirectoryCallback
}

type INodes struct {
	lock        sync.Mutex
	nextINode   INode
	freeINodes  []INode
	inodeStates map[INode]*INodeState

	blocks    *Blocks
	blockSize int64

	workDir string
}

type BlocksDiagnostics struct {
	BlocksInUse  int
	FreeBlockIDs int
	Dir          string
}

type INodesDiagnostics struct {
	NextINode   INode
	FreeINodes  int
	INodesInUse int
	Blocks      *BlocksDiagnostics
	BlockSize   int64
	WorkDir     string
}

func (inodes *INodes) GetDiagnostics() interface{} {
	inodes.lock.Lock()
	defer inodes.lock.Unlock()

	return &INodesDiagnostics{NextINode: inodes.nextINode,
		FreeINodes:  len(inodes.freeINodes),
		INodesInUse: len(inodes.inodeStates),
		Blocks:      inodes.blocks.GetDiagnostics(),
		BlockSize:   inodes.blockSize,
		WorkDir:     inodes.workDir,
	}
}

func (b *Blocks) GetDiagnostics() *BlocksDiagnostics {
	b.lock.Lock()
	defer b.lock.Unlock()

	return &BlocksDiagnostics{BlocksInUse: len(b.blockStates), FreeBlockIDs: len(b.freeBlockID), Dir: b.dir}
}

////////////////////

// type BlockRange struct {
// 	blockID BlockID
// 	offset  uint32
// 	length  uint32
// }

type DirEntry struct {
	Name  string
	INode INode
}

type ExtendedDirEntry struct {
	Name  string
	INode INode
	Size  int64
	IsDir bool
}

type DirEntries struct {
	byName    map[string]INode
	populated map[string]bool
}

func NewDirEntries(inode INode, parentINode INode) *DirEntries {
	d := &DirEntries{byName: make(map[string]INode), populated: make(map[string]bool)}
	d.byName["."] = inode
	d.byName[".."] = parentINode
	d.populated["."] = true
	d.populated[".."] = true

	return d
}

func (d *DirEntries) Get() []ExtendedDirEntry {
	result := make([]ExtendedDirEntry, 0, len(d.byName))
	for name, inode := range d.byName {
		result = append(result, ExtendedDirEntry{Name: name, INode: inode})
	}
	return result
}

func (d *DirEntries) SetEntry(name string, inode INode) {
	if inode != UNALLOCATED_BLOCK_ID {
		d.byName[name] = inode
	}
	d.populated[name] = true
}

func (d *DirEntries) Set(entries []DirEntry) {
	for _, entry := range entries {
		d.SetEntry(entry.Name, entry.INode)
	}
}

func (d *DirEntries) IsPopulated(name string) bool {
	return d.populated[name]
}

func (d *DirEntries) Lookup(name string) (INode, error) {
	inode, ok := d.byName[name]
	if !ok {
		return 0, INVALID_NAME
	}
	return inode, nil
}
