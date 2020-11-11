package cbpatch

import (
	"cloud.google.com/go/storage"
	"compress/zlib"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type Categories struct {
	StorageBucketName string
	RelativeFilePath  string
	RemoteFilePath    string
	Dir               string
	FileName          string
	File              *os.File
	ZippedFileName    string
	ZippedFile        *os.File
	ZippedHash        string
	UnzippedHash      string
	CategoryItems     []CategoriesItem
	ErrorHandler      ErrorHandler
	Logger            Logger
	Storage           Storage
}

func NewCategories(
	errorHandler ErrorHandler,
	logger Logger,
	storage Storage,
	storageBucketName,
	relativeFilePath,
	remoteFilePath,
	dir string,
	zippedHash,
	unzippedHash string,
	categoryItems []CategoriesItem,
) *Categories {
	return &Categories{
		ErrorHandler:      errorHandler,
		Logger:            logger,
		Storage:           storage,
		StorageBucketName: storageBucketName,
		RelativeFilePath:  relativeFilePath,
		RemoteFilePath:    remoteFilePath,
		FileName:          "categories.csv",
		ZippedFileName:    "categories.csv.zlib",
		Dir:               dir,
		ZippedHash:        zippedHash,
		UnzippedHash:      unzippedHash,
		CategoryItems:     categoryItems,
	}
}

func (c *Categories) Init() error {
	unzippedFilePath := filepath.Join(c.Dir, c.FileName)
	zippedFilePath := filepath.Join(c.Dir, c.ZippedFileName)
	c.Logger.DebugF("debug", "initialising categories files: Unzipped: %s. Zipped: %s", unzippedFilePath, zippedFilePath)
	var e error
	c.File, e = os.Create(unzippedFilePath)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	c.ZippedFile, e = os.Create(zippedFilePath)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (c *Categories) Download() error {
	c.Logger.DebugF("debug", "downloading categories from: %s", c.RemoteFilePath)
	e := c.Storage.DownloadWriter(c.StorageBucketName, c.RemoteFilePath, c.ZippedFile)
	if e != nil && !errors.Is(e, storage.ErrObjectNotExist) {
		c.ErrorHandler.Error(e)
		return e
	}

	checksum, e := checksumReadSeeker(c.ZippedFile)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	if c.ZippedHash != checksum {
		e := fmt.Errorf("invalid zipped checksum for %s", c.RemoteFilePath)
		c.ErrorHandler.Error(e)
		return e
	}
	c.Logger.DebugF("debug", "zipped file matched master.csv checksum: %s", c.ZippedHash)

	_, e = c.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	bucketZipReader, e := zlib.NewReader(c.ZippedFile)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	_, e = io.Copy(c.File, bucketZipReader)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	checksum, e = checksumReadSeeker(c.File)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	if c.UnzippedHash != checksum {
		e := fmt.Errorf("invalid unzipped checksum for %s", c.RemoteFilePath)
		c.ErrorHandler.Error(e)
		return e
	}
	c.Logger.DebugF("debug", "unzipped file matched master.csv checksum: %s", c.UnzippedHash)

	// verify bucket contents
	_, e = c.File.Seek(0, io.SeekStart)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	c.Logger.DebugF("debug", "verifying categories")
	verifyBucketReader := csv.NewReader(c.File)
	for true {
		line, e := verifyBucketReader.Read()

		if e == io.EOF {
			break
		}

		if len(line) != 4 {
			e := fmt.Errorf("categories contained a row which was not 4 columns: %s", c.RemoteFilePath)
			c.ErrorHandler.Error(e)
			return e
		}
	}

	_, e = c.File.Seek(0, io.SeekStart)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (c *Categories) Write() error {
	e := c.File.Truncate(0)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	_, e = c.File.Seek(0, io.SeekStart)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	categoriesCsv := csv.NewWriter(c.File)
	for _, definition := range c.CategoryItems {
		e := categoriesCsv.Write([]string{
			strconv.Itoa(definition.GetId()),
			definition.GetCategory(),
			definition.GetSubcategory(),
			definition.GetDescription(),
		})
		if e != nil {
			c.ErrorHandler.Error(e)
			return e
		}
	}
	categoriesCsv.Flush()
	e = categoriesCsv.Error()
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (c *Categories) IsChanged() (bool, error) {
	computed, e := checksumReadSeeker(c.File)
	if e != nil {
		return false, e
	}

	return computed != c.UnzippedHash, nil
}

func (c *Categories) Compress() error {
	e := c.ZippedFile.Truncate(0)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	_, e = c.File.Seek(0, io.SeekStart)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	_, e = c.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	bucketZipWriter := zlib.NewWriter(c.ZippedFile)
	_, e = io.Copy(bucketZipWriter, c.File)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	e = bucketZipWriter.Close()
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (c *Categories) Hash() error {
	var e error
	c.UnzippedHash, e = checksumReadSeeker(c.File)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	c.ZippedHash, e = checksumReadSeeker(c.ZippedFile)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (c *Categories) Upload(public bool) error {
	_, e := c.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	c.Logger.DebugF("debug", "Uploading to: %s", c.RemoteFilePath)

	storageWriter, e := c.Storage.GetUploadWriter(
		c.StorageBucketName,
		c.RemoteFilePath,
	)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	_, e = io.Copy(storageWriter, c.ZippedFile)
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}
	e = storageWriter.Close()
	if e != nil {
		c.ErrorHandler.Error(e)
		return e
	}

	if public {
		e = c.Storage.MakePublic(
			c.StorageBucketName,
			c.RemoteFilePath,
		)
		if e != nil {
			c.ErrorHandler.Error(e)
			return e
		}
	}

	return nil
}
