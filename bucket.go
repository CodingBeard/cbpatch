package cbpatch

import (
	"cloud.google.com/go/storage"
	"compress/zlib"
	"crypto/md5"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type Bucket struct {
	StorageBucketName string
	RelativeFilePath  string
	RemoteFilePath    string
	Dir               string
	Number            int
	FileName          string
	File              *os.File
	ZippedFileName    string
	ZippedFile        *os.File
	IsChanged         bool
	IsDeleted         bool
	ZippedHash        string
	UnzippedHash      string
	PatchCount        int
	Validation        func(line []string, bucket *Bucket) error
	Patches           []Patch
	ErrorHandler      ErrorHandler
	Logger            Logger
	Storage           Storage
}

func NewBucket(
	errorHandler ErrorHandler,
	logger Logger,
	storage Storage,
	storageBucketName,
	relativeFilePath,
	remoteFilePath,
	dir string,
	number int,
	zippedHash,
	unzippedHash string,
	patchCount int,
	validation func(line []string, bucket *Bucket) error,
) *Bucket {
	return &Bucket{
		ErrorHandler: errorHandler,
		Logger: logger,
		Storage: storage,
		StorageBucketName: storageBucketName,
		RelativeFilePath:  relativeFilePath,
		RemoteFilePath:    remoteFilePath,
		Number:            number,
		FileName:          strconv.Itoa(number) + ".csv",
		ZippedFileName:    strconv.Itoa(number) + ".csv.zlib",
		Dir:               dir,
		ZippedHash:        zippedHash,
		UnzippedHash:      unzippedHash,
		PatchCount:        patchCount,
		Validation:        validation,
	}
}

func (b *Bucket) Init() error {
	unzippedFilePath := filepath.Join(b.Dir, b.FileName)
	zippedFilePath := filepath.Join(b.Dir, b.ZippedFileName)
	b.Logger.DebugF("debug", "initialising bucket files: Unzipped: %s. Zipped: %s", unzippedFilePath, zippedFilePath)
	var e error
	b.File, e = os.OpenFile(unzippedFilePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	b.ZippedFile, e = os.OpenFile(zippedFilePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (b *Bucket) Download() error {
	b.Logger.DebugF("debug", "downloading bucket from: %s", b.RemoteFilePath)
	e := b.ZippedFile.Truncate(0)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	_, e = b.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	e = b.Storage.DownloadWriter(b.StorageBucketName, b.RemoteFilePath, b.ZippedFile)
	if e != nil && !errors.Is(e, storage.ErrObjectNotExist) {
		b.ErrorHandler.Error(e)
		return e
	}

	e = b.Unzip()
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	e = b.VerifyUnzipped()
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (b *Bucket) Unzip() error {
	// verify zipped buckets against master
	checksum, e := checksumReadSeeker(b.ZippedFile)
	if e != nil {
		return e
	}
	if b.ZippedHash != checksum {
		e := fmt.Errorf("invalid zipped checksum for %s", b.RemoteFilePath)
		return e
	}
	b.Logger.DebugF("debug", "zipped file matched master.csv checksum: %s", b.ZippedHash)

	_, e = b.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		return e
	}
	bucketZipReader, e := zlib.NewReader(b.ZippedFile)
	if e != nil {
		return e
	}
	e = b.File.Truncate(0)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	_, e = b.File.Seek(0, io.SeekStart)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	_, e = io.Copy(b.File, bucketZipReader)
	if e != nil {
		return e
	}

	return nil
}

func (b *Bucket) VerifyUnzipped() error {
	// verify unzipped buckets against master
	checksum, e := checksumReadSeeker(b.File)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	if b.UnzippedHash != checksum {
		e := fmt.Errorf("invalid unzipped checksum for %s", b.RemoteFilePath)
		return e
	}
	b.Logger.DebugF("debug", "unzipped file matched master.csv checksum: %s", b.UnzippedHash)

	// verify bucket contents
	_, e = b.File.Seek(0, io.SeekStart)
	if e != nil {
		return e
	}

	b.Logger.DebugF("debug", "verifying bucket")
	verifyBucketReader := csv.NewReader(b.File)
	for true {
		line, e := verifyBucketReader.Read()

		if e == io.EOF {
			break
		}

		validationE := b.Validation(line, b)
		if validationE != nil {
			return validationE
		}

		if len(line) < 2 {
			e := fmt.Errorf("bucket contained a row with less than 2 elements: %s", b.RemoteFilePath)
			return e
		}

		if len(line) == 2 {
			b.Patches = append(b.Patches, &DefaultPatch{
				Action: line[0],
				Key:    line[1],
				Values: []string{},
			})
		} else {
			b.Patches = append(b.Patches, &DefaultPatch{
				Action: line[0],
				Key:    line[1],
				Values: line[2:],
			})
		}

	}

	_, e = b.File.Seek(0, io.SeekStart)
	if e != nil {
		return e
	}

	return nil
}

func (b *Bucket) AddPatch(patch Patch) error {
	_, e := b.File.Seek(0, io.SeekEnd)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	patchWriter := csv.NewWriter(b.File)

	e = patchWriter.Write(append([]string{
		patch.GetAction(),
		patch.GetKey(),
	}, patch.GetValues()...))
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	patchWriter.Flush()
	if patchWriter.Error() != nil {
		return patchWriter.Error()
	}

	b.IsChanged = true
	b.Patches = append(b.Patches, patch)
	return nil
}

func (b *Bucket) IsFull() (bool, error) {
	position, e := b.File.Seek(0, io.SeekEnd)
	if e != nil {
		b.ErrorHandler.Error(e)
		return false, e
	}

	return position > 2048000, nil
}

func (b *Bucket) Compress() error {
	e := b.ZippedFile.Truncate(0)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	_, e = b.File.Seek(0, io.SeekStart)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	_, e = b.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	bucketZipWriter := zlib.NewWriter(b.ZippedFile)
	_, e = io.Copy(bucketZipWriter, b.File)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	e = bucketZipWriter.Close()
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (b *Bucket) VerifyZipped() error {
	_, e := b.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	bucketZipReader, e := zlib.NewReader(b.ZippedFile)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	b.Logger.DebugF("debug", "verifying zipped bucket")
	verifyBucketReader := csv.NewReader(bucketZipReader)
	for true {
		line, e := verifyBucketReader.Read()

		if e == io.EOF {
			break
		}

		validationE := b.Validation(line, b)
		if validationE != nil {
			return validationE
		}
	}

	return nil
}

func (b *Bucket) Hash() error {
	var e error
	b.UnzippedHash, e = checksumReadSeeker(b.File)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	b.ZippedHash, e = checksumReadSeeker(b.ZippedFile)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	return nil
}

func (b *Bucket) Upload(public bool) error {
	_, e := b.ZippedFile.Seek(0, io.SeekStart)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	b.Logger.DebugF("debug", "Uploading to: %s", b.RemoteFilePath)

	storageWriter, e := b.Storage.GetUploadWriter(
		b.StorageBucketName,
		b.RemoteFilePath,
	)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	_, e = io.Copy(storageWriter, b.ZippedFile)
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}
	e = storageWriter.Close()
	if e != nil {
		b.ErrorHandler.Error(e)
		return e
	}

	if public {
		e = b.Storage.MakePublic(
			b.StorageBucketName,
			b.RemoteFilePath,
		)
		if e != nil {
			b.ErrorHandler.Error(e)
			return e
		}
	}

	return nil
}

func checksumReadSeeker(seeker io.ReadSeeker) (string, error) {
	_, e := seeker.Seek(0, io.SeekStart)
	if e != nil {
		return "", e
	}

	hash := md5.New()
	if _, e := io.Copy(hash, seeker); e != nil {
		return "", e
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
