package cbpatch

import (
	"bytes"
	"cloud.google.com/go/storage"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
)

type ErrorHandler interface {
	Error(e error)
}

type defaultErrorHandler struct{}

func (d defaultErrorHandler) Error(e error) {
	buf := make([]byte, 1000000)
	runtime.Stack(buf, false)
	buf = bytes.Trim(buf, "\x00")
	stack := string(buf)
	stackParts := strings.Split(stack, "\n")
	newStackParts := []string{stackParts[0]}
	newStackParts = append(newStackParts, stackParts[3:]...)
	stack = strings.Join(newStackParts, "\n")
	log.Println("ERROR", e.Error()+"\n"+stack)
}

type Logger interface {
	InfoF(category string, message string, args ...interface{})
	DebugF(category string, message string, args ...interface{})
}

type defaultLogger struct{}

func (d defaultLogger) InfoF(category string, message string, args ...interface{}) {
	log.Println(category+":", fmt.Sprintf(message, args...))
}

func (d defaultLogger) DebugF(category string, message string, args ...interface{}) {
	log.Println(category+":", fmt.Sprintf(message, args...))
}

type Storage interface {
	DownloadWriter(bucket string, name string, writer io.Writer) error
	GetUploadWriter(bucket string, name string) (*storage.Writer, error)
	MakePublic(bucket string, name string) error
	GetDownloadReader(bucket string, name string) (io.ReadCloser, error)
	Ls(bucket string, dir string) ([]*storage.ObjectAttrs, error)
	Delete(bucket string, filePath string) error
}

type CategoriesItem interface {
	GetId() int
	GetCategory() string
	GetSubcategory() string
	GetIdentifier() string
	GetDescription() string
	GetTlc() string
	GetSlc() string
}