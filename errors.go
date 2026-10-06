package scioredb

import "errors"

var (
	ErrNegativeOffset    = errors.New("negative offset")
	ErrIndexOutOfBounds  = errors.New("index out of bounds")
	ErrFileManagerClosed = errors.New("filemanager is closed")
	ErrBufferTimeout     = errors.New("page buffer pool: timeout waiting for an available buffer")
)
