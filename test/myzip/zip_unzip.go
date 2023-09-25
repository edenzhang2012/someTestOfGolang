package myzip

import (
	"archive/zip"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Zip compresses the specified files or dirs to zip archive.
// If a path is a dir don't need to specify the trailing path separator.
// For example calling Zip("archive.zip", "dir", "csv/baz.csv") will get archive.zip and the content of which is
// baz.csv
// dir
// ├── bar.txt
// └── foo.txt
// Note that if a file is a symbolic link it will be skipped.
func Zip(zipPath string, key []byte, paths ...string) error {
	// Create zip file and it's parent dir.
	if err := os.MkdirAll(filepath.Dir(zipPath), os.ModePerm); err != nil {
		return err
	}
	outFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	var zipWriter *zip.Writer
	if key != nil {
		block, err := aes.NewCipher([]byte(key))
		if err != nil {
			return err
		}

		// 创建一个 AES 加密流
		stream := cipher.NewOFB(block, []byte("0123456789abcdef"))
		writer := &cipher.StreamWriter{S: stream, W: outFile}
		zipWriter = zip.NewWriter(writer)
		defer zipWriter.Close()
	} else {
		zipWriter = zip.NewWriter(outFile)
		defer zipWriter.Close()
	}

	// Traverse the file or directory.
	for _, rootPath := range paths {
		// Remove the trailing path separator if path is a directory.
		rootPath = strings.TrimSuffix(rootPath, string(os.PathSeparator))
		// Visit all the files or directories in the tree.
		err = filepath.Walk(rootPath, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// If a file is a symbolic link it will be skipped.
			//TODO how to deal with symbolic link
			if info.Mode()&os.ModeSymlink != 0 {
				return nil
			}

			// Create a local file header.
			header, err := zip.FileInfoHeader(info)
			if err != nil {
				return err
			}

			// Set compression method.
			header.Method = zip.Deflate

			// Set relative path of a file as the header name.
			header.Name, err = filepath.Rel(filepath.Dir(rootPath), path)
			if err != nil {
				return err
			}
			if info.IsDir() {
				header.Name += string(os.PathSeparator)
			}

			// Create writer for the file header and save content of the file.
			headerWriter, err := zipWriter.CreateHeader(header)
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = io.Copy(headerWriter, f)
			return err
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type unbufferedReaderAt struct {
	R io.Reader
	N int64
}

func NewUnbufferedReaderAt(r io.Reader) io.ReaderAt {
	return &unbufferedReaderAt{R: r}
}

func (u *unbufferedReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off < u.N {
		return 0, errors.New("invalid offset")
	}
	diff := off - u.N
	written, err := io.CopyN(ioutil.Discard, u.R, diff)
	u.N += written
	if err != nil {
		return 0, err
	}

	n, err = u.R.Read(p)
	u.N += int64(n)
	return
}

// Unzip decompresses a zip file to specified directory.
// Note that the destination directory don't need to specify the trailing path separator.
// If the destination directory doesn't exist, it will be created automatically.
func Unzip(zipath, dir string, key []byte) error {
	// Open zip file.
	file, err := os.Open(zipath)
	// reader, err := zip.OpenReader(zipath)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return err
	}

	var r io.Reader
	if key != nil {
		block, err := aes.NewCipher([]byte(key))
		if err != nil {
			return err
		}

		// 创建一个 AES 加密流
		stream := cipher.NewOFB(block, []byte("0123456789abcdef"))
		r = &cipher.StreamReader{S: stream, R: file}
	} else {
		r = file
	}

	reader, err := zip.NewReader(NewUnbufferedReaderAt(r), fi.Size())
	if err != nil {
		return err
	}

	for _, file := range reader.File {
		if err := unzipFile(file, dir); err != nil {
			return err
		}
	}
	return nil
}

func unzipFile(file *zip.File, dir string) error {
	// Prevent path traversal vulnerability.
	// Such as if the file name is "../../../path/to/file.txt" which will be cleaned to "path/to/file.txt".
	name := strings.TrimPrefix(filepath.Join(string(filepath.Separator), file.Name), string(filepath.Separator))
	filePath := path.Join(dir, name)

	// Create the directory of file.
	if file.FileInfo().IsDir() {
		if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}

	// Open the file.
	r, err := file.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	// Create the file.
	w, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer w.Close()

	// Save the decompressed file content.
	_, err = io.Copy(w, r)
	return err
}

// 16,24,32
func Encrypt(src, dst, passwd string) error {
	// 打开要加密的压缩包文件
	zipFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	// 创建一个新的加密的压缩包文件
	encryptedZipFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer encryptedZipFile.Close()

	block, err := aes.NewCipher([]byte(passwd))
	if err != nil {
		return err
	}

	// 创建一个 AES 加密流
	stream := cipher.NewOFB(block, []byte("0123456789abcdef"))
	writer := &cipher.StreamWriter{S: stream, W: encryptedZipFile}

	_, err = io.Copy(writer, zipFile)
	if err != nil {
		return err
	}

	return nil
}

func UnEncrypt(src, dst, passwd string) error {
	encryptedFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer encryptedFile.Close()

	file, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer file.Close()

	block, err := aes.NewCipher([]byte(passwd))
	if err != nil {
		return err
	}

	// 创建一个 AES 加密流
	stream := cipher.NewOFB(block, []byte("0123456789abcdef"))
	reader := &cipher.StreamReader{S: stream, R: encryptedFile}

	_, err = io.Copy(file, reader)
	if err != nil {
		return err
	}

	return nil
}
