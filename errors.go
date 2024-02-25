package treeply

import "errors"

var INVALID_HANDLE = errors.New("Invalid handle")
var IS_DIR = errors.New("Is directory")
var INVALID_NAME = errors.New("Invalid Name")
var INVALID_INODE = errors.New("Invalid INode")
var IS_NOT_DIR = errors.New("INode is not a directory")
