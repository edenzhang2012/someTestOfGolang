package s3

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type myProvider struct {
	creds   credentials.Value
	expired bool
	err     error
}

func (s *myProvider) Retrieve() (credentials.Value, error) {
	s.expired = false
	s.creds.ProviderName = "myProvider"
	return s.creds, s.err
}

func (s *myProvider) IsExpired() bool {
	return s.expired
}

type FileDir struct {
	FDName       string    `json:"name"`
	FDSize       int64     `json:"size"`
	Type         string    `json:"type"`
	LastModify   time.Time `json:"last_modify"`
	StorageClass string    `json:"storage_class"`
}

func (f FileDir) Name() string {
	return f.FDName
}

func (f FileDir) Size() int64 {
	return f.FDSize
}

func (f FileDir) Mode() os.FileMode {
	return 0
}

func (f FileDir) ModTime() time.Time {
	return f.LastModify
}

func (f FileDir) IsDir() bool {
	return f.Type == "dir"
}

func (f FileDir) Sys() interface{} {
	return nil
}

func QueryExternalDataSubdirectoryContents(endpoint, ak, sk, region, bucket, path string, pageSize, pageNum int) ([]os.FileInfo, int, error) {
	var fds []os.FileInfo
	var total int
	var err error
	//query bucket or subdirectory contents
	if bucket == "" { //query bucket
		fds, total, err = ListExternalDataBucketPages(endpoint, ak, sk, region, pageSize, pageNum)
		if err != nil {
			return nil, 0, err
		}
	} else { //query subdirectory contents
		fds, total, err = ListExternalDataFilesAndDirsPages(endpoint, ak, sk, region, bucket, path, pageSize, pageNum)
		if err != nil {
			return nil, 0, err
		}
	}

	if len(fds) == 0 {
		return nil, 0, nil
	}
	return fds, total, nil
}

func ListExternalDataBucketPages(endpoint, ak, sk, region string, pageSize, pageNum int) ([]os.FileInfo, int, error) {
	sess, _ := createExternalS3Session(endpoint, region, ak, sk)
	svc := s3.New(sess)

	result, err := svc.ListBuckets(nil)
	if err != nil {
		fmt.Printf("ListBuckets error: %v", err)
		return nil, 0, err
	}

	var fds []os.FileInfo
	for _, b := range result.Buckets {
		fds = append(fds, &FileDir{
			FDName: aws.StringValue(b.Name),
			FDSize: 0,
			Type:   "bucket",
		})
	}

	total := len(fds)
	if total <= 0 || (pageNum > (total+pageSize-1)/pageSize) {
		fmt.Println("total=0 or pageNum no-sense", "total", total, "pageSize", pageSize, "pageNum", pageNum)
		return nil, total, nil
	}
	startIndex := pageSize * (pageNum - 1)
	endIndex := startIndex + pageSize
	if endIndex > total {
		endIndex = total
	}
	return fds[startIndex:endIndex], total, nil
}

func ListExternalDataFilesAndDirsPages(endpoint, ak, sk, region, bucket, dir string, pageSize, pageNum int) ([]os.FileInfo, int, error) {
	dir = strings.TrimLeft(dir, "/")
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	if dir == "/" {
		dir = ""
	}
	sess, err := createExternalS3Session(endpoint, region, ak, sk)
	if err != nil {
		fmt.Printf("createExternalS3Session failed with: %v", err)
		return nil, 0, err
	}
	svc := s3.New(sess)
	var files []os.FileInfo
	var dirs []os.FileInfo
	page := 1
	err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket),
		Prefix:       aws.String(dir),
		Delimiter:    aws.String("/"),
		MaxKeys:      aws.Int64(int64(pageSize) + 2),
		EncodingType: aws.String(s3.EncodingTypeUrl),
	}, func(p *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range p.Contents {
			decodeName, err := url.QueryUnescape(*item.Key)
			if err != nil {
				decodeName = *item.Key
			}
			if decodeName != dir {
				fmt.Printf("##### get file name %s\n", decodeName)
				files = append(files, &FileDir{
					FDName:       decodeName,
					FDSize:       *item.Size,
					Type:         "file",
					LastModify:   *item.LastModified,
					StorageClass: *item.StorageClass,
				})
			}
		}
		for _, item := range p.CommonPrefixes {
			decodeName, err := url.QueryUnescape(*item.Prefix)
			if err != nil {
				decodeName = *item.Prefix
			}
			fmt.Printf("@@@@@ get file name %s\n", decodeName)
			dirs = append(dirs, &FileDir{
				FDName: decodeName,
				FDSize: 0,
				Type:   "dir",
			})
		}
		if page >= pageNum {
			return false
		} else {
			page++
			return true
		}
	})
	if err != nil {
		return nil, 0, err
	}

	total := len(files) + len(dirs)
	if total <= 0 || (pageNum > (total+pageSize-1)/pageSize) {
		fmt.Println("total=0 or pageNum no-sense", "total", total, "pageSize", pageSize, "pageNum", pageNum)
		return nil, total, nil
	}
	startIndex := pageSize * (pageNum - 1)
	endIndex := startIndex + pageSize
	if endIndex > total {
		endIndex = total
	}

	fileDirs := append(dirs, files...)
	return fileDirs[startIndex:endIndex], total, nil
}

func createExternalS3Session(endpoint, region, accessKeyID, secretAccessKey string) (*session.Session, error) {
	return session.NewSession(&aws.Config{
		Credentials: credentials.NewCredentials(&myProvider{
			creds: credentials.Value{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
				//SessionToken:    "",
			},
			expired: false,
		}),
		Region:   aws.String(region),
		Endpoint: aws.String(endpoint),
		HTTPClient: func() *http.Client {
			return &http.Client{
				Transport: func() *http.Transport {
					return &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
				}(),
				Timeout: 3 * time.Second,
			}
		}(),
		S3ForcePathStyle: func() *bool { b := true; return &b }()},
	)
}
