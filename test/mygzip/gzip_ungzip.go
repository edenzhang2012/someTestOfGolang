package mygzip

import (
	"archive/tar"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

const (
	NoCompression      = gzip.NoCompression
	BestSpeed          = gzip.BestSpeed
	BestCompression    = gzip.BestCompression
	DefaultCompression = gzip.DefaultCompression
	HuffmanOnly        = gzip.HuffmanOnly

	AES128 = 16
	AES192 = 24
	AES256 = 32
)

/*
level can be : NoCompression,BestSpeed,BestCompression,DefaultCompression,HuffmanOnly
*/
func Gzip(dst string, compressLevel, encryptType int, key []byte, src ...string) error {
	if len(src) == 0 {
		return fmt.Errorf("no file specified")
	}
	zipFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	var gzipWriter *gzip.Writer
	if key != nil {
		if encryptType != AES128 && encryptType != AES192 && encryptType != AES256 {
			return fmt.Errorf("encryptType not support(support AES128,AES192,AES256)")
		}
		hash := sha256.New()
		if _, err := hash.Write(key); err != nil {
			return err
		}
		block, err := aes.NewCipher(hash.Sum(nil)[:encryptType])
		if err != nil {
			return err
		}

		// 创建一个 AES 加密流
		stream := cipher.NewOFB(block, []byte("0123456789abcdef"))
		writer := &cipher.StreamWriter{S: stream, W: zipFile}
		gzipWriter, err = gzip.NewWriterLevel(writer, compressLevel)
		if err != nil {
			return nil
		}
	} else {
		gzipWriter, err = gzip.NewWriterLevel(zipFile, compressLevel)
		if err != nil {
			return nil
		}
	}
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for _, f := range src {
		err = filepath.Walk(f, func(filePath string, fileInfo os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// generate tar header
			header, err := tar.FileInfoHeader(fileInfo, filePath)
			if err != nil {
				return err
			}

			header.Name = filepath.ToSlash(filePath)

			// write header
			if err := tarWriter.WriteHeader(header); err != nil {
				return err
			}
			// if not a dir, write file content
			if !fileInfo.IsDir() {
				data, err := os.Open(filePath)
				if err != nil {
					return err
				}
				if _, err := io.Copy(tarWriter, data); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

func UnGzip(dst, src string, encryptType int, key []byte) error {
	compressedFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer compressedFile.Close()

	var r io.Reader
	if key != nil {
		if encryptType != AES128 && encryptType != AES192 && encryptType != AES256 {
			return fmt.Errorf("encryptType not support(support AES128,AES192,AES256)")
		}
		hash := sha256.New()
		if _, err := hash.Write(key); err != nil {
			return err
		}
		block, err := aes.NewCipher(hash.Sum(nil)[:encryptType])
		if err != nil {
			return err
		}

		// 创建一个 AES 加密流
		stream := cipher.NewOFB(block, []byte("0123456789abcdef"))
		r = &cipher.StreamReader{S: stream, R: compressedFile}
	} else {
		r = compressedFile
	}

	zr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()

	tr := tar.NewReader(zr)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return err
		}

		target := filepath.Join(dst, header.Name)

		// check the type
		switch header.Typeflag {
		// if its a dir and it doesn't exist create it (with 0755 permission)
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		// if it's a file create it (with same permission)
		case tar.TypeReg:
			fileToWrite, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			defer fileToWrite.Close()
			// copy over contents
			if _, err := io.Copy(fileToWrite, tr); err != nil {
				return err
			}
		}
	}

	return nil
}
