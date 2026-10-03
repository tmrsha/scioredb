package scioredb

import "errors"

var (
	ErrNegativeOffset    = errors.New("negative offset")
	ErrIndexOutOfBounds  = errors.New("index out of bounds")
	ErrFileManagerClosed = errors.New("filemanager is closed")
)
