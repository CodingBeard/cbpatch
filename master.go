package cbpatch

import (
	"encoding/csv"
	"fmt"
	"github.com/codingbeard/cbutil"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	masterFilename = "master.csv"
)

type Master struct {
	StorageBucketName string
	RemoteDir         string
	Dir               string
	FileName          string
	Public            bool
	File              *os.File
	Categories        *Categories
	CategoryItems     []CategoriesItem
	IsChanged         bool
	Version           string
	UnixTime          int64
	DateTime          string
	Validation        func(line []string, bucket *Bucket) error
	Buckets           []*Bucket
	ErrorHandler      ErrorHandler
	Logger            Logger
	Storage           Storage
}

type Config struct {
	StorageBucketName string
	RemoteDir         string
	Dir               string
	FileName          string
	Public            bool
	Validation        func(line []string, bucket *Bucket) error
	CategoryItems     []CategoriesItem
	ErrorHandler      ErrorHandler
	Logger            Logger
	Storage           Storage
}

func NewMaster(config Config) *Master {
	master := Master{
		StorageBucketName: config.StorageBucketName,
		RemoteDir:         config.RemoteDir,
		Dir:               config.Dir,
		FileName:          config.FileName,
		Public:            config.Public,
		Validation:        config.Validation,
		CategoryItems:     config.CategoryItems,
		ErrorHandler:      config.ErrorHandler,
		Logger:            config.Logger,
		Storage:           config.Storage,
	}

	return &master
}

func (m *Master) Init() error {
	filePath := filepath.Join(m.Dir, m.FileName)
	m.Logger.DebugF("debug", "initialising master: %s", filePath)
	var e error
	m.File, e = os.Create(filePath)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}
	return nil
}

func (m *Master) Download() error {
	if m.Public {
		masterUrl := fmt.Sprintf(
			"https://storage.googleapis.com/%s/%s/%s?%d",
			m.StorageBucketName,
			m.RemoteDir,
			m.FileName,
			time.Now().Unix(),
		)

		m.Logger.DebugF("debug", "downloading master from: %s", masterUrl)

		response, e := http.Get(masterUrl)
		if e != nil {
			if !strings.Contains(e.Error(), "404") {
				m.ErrorHandler.Error(e)
				return e
			}
		} else {
			defer response.Body.Close()
			_, e = io.Copy(m.File, response.Body)
		}
	} else {
		reader, e := m.Storage.GetDownloadReader(m.StorageBucketName, joinPath(m.RemoteDir, m.FileName))
		if e != nil {
			if !strings.Contains(e.Error(), "object doesn't exist") {
				m.ErrorHandler.Error(e)
				return e
			}
		} else {
			_, e = io.Copy(m.File, reader)
		}
	}

	_, e := m.File.Seek(0, io.SeekStart)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}
	reader := csv.NewReader(m.File)
	for true {
		line, e := reader.Read()

		if e == io.EOF {
			break
		}

		if len(line) == 3 {
			m.Logger.DebugF("debug", "found remote master, datetime: %s", line[2])
			m.Version = line[0]
			m.UnixTime, e = strconv.ParseInt(line[1], 10, 64)
			m.DateTime = line[2]
			continue
		}

		if len(line) == 6 {
			bucketNumber, e := strconv.Atoi(line[0])
			if bucketNumber == -1 {
				m.Categories = NewCategories(
					m.ErrorHandler,
					m.Logger,
					m.Storage,
					m.StorageBucketName,
					line[1],
					joinPath(m.RemoteDir, line[1]),
					m.Dir,
					line[3],
					line[4],
					m.CategoryItems,
				)
				e = m.Categories.Init()
				if e != nil {
					return e
				}
				e = m.Categories.Download()
				if e != nil {
					return e
				}
			} else {
				lineCount, e := strconv.Atoi(line[5])
				if e != nil {
					m.ErrorHandler.Error(e)
					return e
				}
				bucket := NewBucket(
					m.ErrorHandler,
					m.Logger,
					m.Storage,
					m.StorageBucketName,
					line[1],
					joinPath(m.RemoteDir, line[1]),
					m.Dir,
					bucketNumber,
					line[3],
					line[4],
					lineCount,
					m.Validation,
				)
				m.Buckets = append(m.Buckets, bucket)
				m.Logger.DebugF(
					"debug",
					"found a remote bucket (%d): %s/%s. Zipped: %s. Unzipped: %s",
					bucketNumber,
					bucket.StorageBucketName,
					bucket.RemoteFilePath,
					bucket.ZippedHash,
					bucket.UnzippedHash,
				)
			}
		}
	}

	return nil
}

func (m *Master) DownloadBuckets() error {
	m.Logger.DebugF("debug", "downloading buckets")
	for _, bucket := range m.Buckets {
		e := bucket.Init()
		if e != nil {
			m.ErrorHandler.Error(e)
			return e
		}
		e = bucket.VerifyUnzipped()
		if e != nil {
			e = bucket.Download()
			if e != nil {
				m.ErrorHandler.Error(e)
				return e
			}
		}
	}

	return nil
}

func (m *Master) CompileList() (map[string][]string, error) {
	m.Logger.DebugF("debug", "compiling bucket list")
	totalPatches := 0
	list := make(map[string][]string)
	for _, bucket := range m.Buckets {
		if bucket.IsDeleted {
			continue
		}
		totalPatches += len(bucket.Patches)
		for _, patch := range bucket.Patches {
			switch patch.GetAction() {
			case "+":
				list[patch.GetKey()] = patch.GetValues()
				break
			case "-":
				delete(list, patch.GetKey())
				break
			case "*":
				list = make(map[string][]string)
				break
			}
		}
	}

	m.Logger.InfoF("LIST", "Total patches: %d, total compiled length: %d", totalPatches, len(list))

	return list, nil
}

type change struct {
	key       string
	bucketKey int
	patchKey  int
}

func (m *Master) CalculateWastage() (int, error) {
	m.Logger.DebugF("debug", "Calculating wastage")
	changes := make(map[string][]change)
	for bucketKey, bucket := range m.Buckets {
		for patchKey, patch := range bucket.Patches {
			if _, ok := changes[patch.GetKey()]; !ok {
				changes[patch.GetKey()] = []change{{
					key:       patch.GetKey(),
					bucketKey: bucketKey,
					patchKey:  patchKey,
				}}
			} else {
				changes[patch.GetKey()] = append(changes[patch.GetKey()], change{
					key:       patch.GetKey(),
					bucketKey: bucketKey,
					patchKey:  patchKey,
				})
			}
		}
	}

	var wastage []change

	for _, history := range changes {
		if len(history) > 1 {
			if len(history)%2 == 0 {
				// no longer in list
				wastage = append(wastage, history...)
			} else {
				// in list
				wastage = append(wastage, history[:len(history)-1]...)
			}
		}
	}

	wastePercent := int((float64(len(wastage)) / float64(len(changes))) * 100)

	m.Logger.DebugF("debug", "%d items wasted %d%%", len(wastage), wastePercent)

	return wastePercent, nil
}

func (m *Master) AddPatch(patch Patch) error {
	maxBucketNumber := 0
	var latestBucket *Bucket
	for _, bucket := range m.Buckets {
		if bucket.Number > maxBucketNumber {
			maxBucketNumber = bucket.Number
			latestBucket = bucket
		}
	}

	var full bool
	var e error
	if latestBucket != nil {
		full, e = latestBucket.IsFull()
		if e != nil {
			m.ErrorHandler.Error(e)
			return e
		}
	}

	if latestBucket == nil || full {
		latestBucket = NewBucket(
			m.ErrorHandler,
			m.Logger,
			m.Storage,
			m.StorageBucketName,
			"",
			"",
			m.Dir,
			maxBucketNumber+1,
			"",
			"",
			0,
			m.Validation,
		)
		e := latestBucket.Init()
		if e != nil {
			return e
		}
		m.Buckets = append(m.Buckets, latestBucket)
	}

	return latestBucket.AddPatch(patch)
}

func (m *Master) InitCategories() error {
	if m.Categories == nil {
		m.Categories = NewCategories(
			m.ErrorHandler,
			m.Logger,
			m.Storage,
			m.StorageBucketName,
			"",
			"",
			m.Dir,
			"",
			"",
			m.CategoryItems,
		)
		e := m.Categories.Init()
		if e != nil {
			return e
		}
	}

	return nil
}

func (m *Master) UploadToStorageBucket() error {
	m.Logger.DebugF("debug", "uploading to storage bucket")
	now := time.Now()
	m.UnixTime = now.Unix()
	m.DateTime = now.Format(cbutil.DateTimeFormat)

	e := m.File.Truncate(0)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}
	_, e = m.File.Seek(0, io.SeekStart)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}

	masterCsv := csv.NewWriter(m.File)
	e = masterCsv.Write([]string{
		"V1",
		strconv.FormatInt(m.UnixTime, 10),
		m.DateTime,
	})
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}
	masterCsv.Flush()
	e = masterCsv.Error()
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}

	if m.Categories != nil {
		e = m.Categories.Write()
		if e != nil {
			return e
		}

		categoriesChanged, e := m.Categories.IsChanged()
		if e != nil {
			return e
		}
		if categoriesChanged {
			e = m.Categories.Compress()
			if e != nil {
				return e
			}
			e = m.Categories.Hash()
			if e != nil {
				return e
			}
			m.Categories.RelativeFilePath = strconv.FormatInt(m.UnixTime, 10) + "-" + m.Categories.ZippedFileName
			m.Categories.RemoteFilePath = joinPath(
				m.RemoteDir,
				m.Categories.RelativeFilePath,
			)

			e = m.Categories.Upload(m.Public)
			if e != nil {
				return e
			}
		}

		e = masterCsv.Write([]string{
			"-1",
			m.Categories.RelativeFilePath,
			"zlib",
			m.Categories.ZippedHash,
			m.Categories.UnzippedHash,
			strconv.Itoa(len(m.CategoryItems)),
		})
		if e != nil {
			m.ErrorHandler.Error(e)
			return e
		}
		masterCsv.Flush()
		e = masterCsv.Error()
		if e != nil {
			m.ErrorHandler.Error(e)
			return e
		}
	}

	for _, bucket := range m.Buckets {
		if bucket.IsDeleted {
			continue
		}
		if bucket.IsChanged {
			m.Logger.DebugF("debug", "bucket has changed, compressing and hashing: %s", bucket.FileName)
			e := bucket.Compress()
			if e != nil {
				return e
			}
			e = bucket.VerifyZipped()
			if e != nil {
				return e
			}
			e = bucket.Hash()
			if e != nil {
				return e
			}

			bucket.RelativeFilePath = strconv.FormatInt(m.UnixTime, 10) + "-" + bucket.ZippedFileName
			bucket.RemoteFilePath = joinPath(
				m.RemoteDir,
				bucket.RelativeFilePath,
			)
			e = bucket.Upload(m.Public)
			if e != nil {
				return e
			}
		}

		e = masterCsv.Write([]string{
			strconv.Itoa(bucket.Number),
			bucket.RelativeFilePath,
			"zlib",
			bucket.ZippedHash,
			bucket.UnzippedHash,
			strconv.Itoa(len(bucket.Patches)),
		})
		if e != nil {
			m.ErrorHandler.Error(e)
			return e
		}
		masterCsv.Flush()
		e = masterCsv.Error()
		if e != nil {
			m.ErrorHandler.Error(e)
			return e
		}

	}

	_, e = m.File.Seek(0, io.SeekStart)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}

	m.Logger.InfoF("debug", "uploading: %s", joinPath(m.RemoteDir, m.FileName))

	storageWriter, e := m.Storage.GetUploadWriter(
		m.StorageBucketName,
		joinPath(m.RemoteDir, m.FileName),
	)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}

	_, e = io.Copy(storageWriter, m.File)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}
	e = storageWriter.Close()
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}

	if m.Public {
		e = m.Storage.MakePublic(
			m.StorageBucketName,
			joinPath(m.RemoteDir, m.FileName),
		)
		if e != nil {
			m.ErrorHandler.Error(e)
			return e
		}
	}

	return nil
}

func (m *Master) CleanupOldFiles() error {
	files, e := m.Storage.Ls(m.StorageBucketName, m.RemoteDir)
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}

	for _, file := range files {
		found := false
		if m.Categories != nil && file.Name == m.Categories.RemoteFilePath {
			found = true
		}
		for _, bucket := range m.Buckets {
			if file.Name == bucket.RemoteFilePath {
				found = true
			}
		}
		if file.Name == m.RemoteDir+"/"+m.FileName {
			found = true
		}
		if !found {
			m.Logger.DebugF("debug", "File found in bucket not in master: %s", file.Name)

			if file.Created.Before(time.Now().Add(-time.Hour * 24)) {
				m.Logger.DebugF("debug", "File is more than 24 hours old, deleting now: %s", file.Name)

				e = m.Storage.Delete(m.StorageBucketName, file.Name)
				if e != nil {
					m.ErrorHandler.Error(e)
					return e
				}
			}
		}
	}

	return nil
}

func (m *Master) CleanupOldLocalFiles() error {
	files, e := filepath.Glob(m.Dir + "/*")
	if e != nil {
		m.ErrorHandler.Error(e)
		return e
	}

	for _, file := range files {
		found := false
		if m.Categories != nil && file == m.Categories.FileName {
			found = true
		}
		for _, bucket := range m.Buckets {
			if file == m.Dir+"/"+bucket.ZippedFileName || file == m.Dir+"/"+bucket.FileName {
				found = true
			}
		}
		if file == m.Dir+"/"+m.FileName {
			found = true
		}
		if !found {
			m.Logger.DebugF("debug", "File found in work dir not in master: %s", file)

			e = os.Remove(file)
			if e != nil {
				m.ErrorHandler.Error(e)
				continue
			}
		}
	}

	return nil
}

func (m *Master) Close() {
	if m.File != nil {
		m.File.Close()
	}

	for _, bucket := range m.Buckets {
		if bucket.File != nil {
			bucket.File.Close()
		}
		if bucket.ZippedFile != nil {
			bucket.ZippedFile.Close()
		}
	}
}

func joinPath(parts ...string) string {
	return strings.Join(parts, "/")
}
