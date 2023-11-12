package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"test/mygzip"
	"time"
)

func trace(s string) {
	pc, _, line, ok := runtime.Caller(1)
	if ok {
		f := runtime.FuncForPC(pc)
		fmt.Println("[" + f.Name() + ":" + strconv.Itoa(line) + "]")
	}
}

func foo(s string) {
	trace(s)
}

func filepath_test1() {
	dir, file := filepath.Split("")
	fmt.Println(dir, file)
	dir, file = filepath.Split("/")
	fmt.Println(dir, file)
	dir, file = filepath.Split("/tmp/")
	fmt.Println(dir, file)
	dir, file = filepath.Split("tmp")
	fmt.Println(dir, file)
	dir, file = filepath.Split("tmp/abc")
	fmt.Println(dir, file)
	dir, file = filepath.Split("tmp/abc/")
	fmt.Println(dir, file)
	dir, file = filepath.Split("/tmp")
	fmt.Println(dir, file)
}

func filepath_test2() {
	dir := "//Gemini-Snapshot/type/space/user/dataset"

	dirs := strings.Split(dir, "/")
	fmt.Println(dirs, len(dirs))
}

func SimpleHttp(ctx context.Context, method, rawUrl string, params *url.Values, headers map[string]string) (body []byte, err error) {
	fmt.Printf("SimpleHttp method %s, haeders %v, rawUrl %s, params %v", method, headers, rawUrl, params)

	u, err := url.ParseRequestURI(rawUrl)
	if err != nil {
		return nil, err
	}

	if params != nil {
		u.RawQuery = params.Encode()
	}

	req, err := http.NewRequest(method, u.String(), nil)
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		req.Header.Add(key, value)
	}

	var res *http.Response
	if ctx == nil {
		res, err = (&http.Client{}).Do(req)
	} else {
		res, err = (&http.Client{}).Do(req.WithContext(ctx))
	}
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	type Response struct {
		Err string `json:"error"`
	}
	var resp Response

	body, err = io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	//not wanted status
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		if res.StatusCode == 404 {
			return nil, os.ErrNotExist
		}

		if len(body) > 0 {
			if err := json.Unmarshal(body, &resp); err != nil {
				fmt.Printf("SimpleHttp unmarshal failed with %v", err)
				return nil, err
			}

			if resp.Err != "" {
				if strings.Contains(resp.Err, "already exist") {
					return nil, os.ErrExist
				}

				if strings.Contains(resp.Err, "not found") || strings.Contains(resp.Err, "not exist") {
					return nil, os.ErrNotExist
				}

				fmt.Printf("SimpleHttp failed with: %s", resp.Err)
				return nil, fmt.Errorf("SimpleHttp failed with: %s", resp.Err)
			}
		}
		return nil, fmt.Errorf("SimpleHttp failed with http status %d and empty body", res.StatusCode)
	}

	//success
	if len(body) > 0 {
		return body, nil
	}

	return nil, nil
}

type SnapLocation string

type Dataset struct {
	DataType string
	Space    string
	User     string
	Name     string

	// Latest       SnapLocation //logical path
	Base         SnapLocation //logical path
	BaseRealPath string

	SnapInfos map[string]*SnapInfo //key is snap name, value is SnapInfo
}

type SnapInfo struct {
	Name          string
	Location      SnapLocation
	AliasLocation SnapLocation
	RealPath      string

	Shared         bool //这个字段仅仅用来标记当前快照层级变为base的child
	ShareCount     int32
	AliasCount     int32
	ChildrenCount  int32
	Children       map[SnapLocation]string //key is snaplocation, value is real path
	LogicalDelete  bool
	Parent         SnapLocation
	ParentRealPath string

	CreateTime time.Time
}

func http_test() {
	// params := url.Values{}
	// params.Add("logicalPath", "traindata/space4/user4/dataset441/latest")
	// params.Add("realPath", "/pavostor/gemini/traindata/user_uuid_1/base1")
	// err := SimpleHttp("POST", "http://localhost:8888/snap/mark", &params, nil)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// var data Dataset
	// params := url.Values{}
	// params.Add("logicalPath", "traindata/space4/user4/dataset552")
	// body, err := SimpleHttp("GET", "http://localhost:8888/snap", &params)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// err = json.Unmarshal(body, &data)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// fmt.Println(data)

	// rawUrl := "http://localhost:8888/pavostor/snap/mark"
	// params := url.Values{}
	// params.Add("logicalPath", "traindata/space1/user1/dataset119/latest")
	// params.Add("realPath", "/pavostor/gemini/traindata/space1/dataset1/id1")
	// err := SimpleHttp("POST", rawUrl, &params, nil)
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Println("success")
	// }

	// rawUrl := "http://localhost:8888/snap"
	// params := url.Values{}
	// params.Add("logicalPath", "traindata/space4/user4/dataset551/^base^")
	// params.Add("get_parents", "true")
	// snaps := make([]*SnapInfo, 0)
	// body, err := SimpleHttp("GET", rawUrl, &params)
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	if err := json.Unmarshal(body, &snaps); err != nil {
	// 		fmt.Printf("unmarshal failed with: %v \n", err)
	// 		return
	// 	}
	// 	fmt.Println(snaps)
	// }

	rawUrl := filerPath("/t123H就sbajd%jsahh.jpg")
	params := url.Values{}
	params.Add("metadata", "true")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, err := SimpleHttp(ctx, "GET", rawUrl, &params, nil)
	if err != nil {
		if err == context.DeadlineExceeded {
			fmt.Println("aaaaaaa")
		}

		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("bbbbbbb")
		}

		if err, ok := err.(*url.Error); ok && err.Timeout() {
			fmt.Println("ccccc")
		}
		fmt.Println(err)
	} else {
		fmt.Println(string(body))
	}
}

func filerPath(path string) string {
	var urlString string
	if strings.HasPrefix(path, "/"+"Gemini-Snapshot") || //快照
		strings.HasPrefix(path, "/registry") || //镜像库
		strings.HasPrefix(path, "/pavostor") || //平台数据前缀
		strings.HasPrefix(path, "/snap") { //快照操作
		urlString = path
	}

	urlString = path
	urlString = strings.TrimPrefix(urlString, "/")
	urlString = url.PathEscape(urlString)

	urlString = "http://localhost:8889" + "/" + urlString
	if !strings.HasPrefix(urlString, "http://") {
		urlString = "http://" + urlString
	}
	return urlString
}

func unmarshal() {
	strs := make([]*string, 0)
	for i := 0; i < 10; i++ {
		tmp := fmt.Sprintf("aaa_%d", i)
		strs = append(strs, &tmp)
	}

	dataByte, err := json.Marshal(strs)
	if err != nil {
		fmt.Println("failed1")
		return
	}
	fmt.Println(string(dataByte))

	strs1 := make([]*string, 0)
	err = json.Unmarshal(dataByte, &strs1)
	if err != nil {
		fmt.Println("failed2")
		return
	}
	for i := 0; i < len(strs1); i++ {
		fmt.Println(*strs1[i])
	}
}

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

	var iv [aes.BlockSize]byte

	// 创建一个 AES 加密流
	stream := cipher.NewOFB(block, iv[:])
	writer := &cipher.StreamWriter{S: stream, W: encryptedZipFile}

	_, err = io.Copy(writer, zipFile)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	// http_test()

	// foo("test")
	// filepath_test2()
	// http_test()
	// unmarshal()

	// str := "/aaa/bbb/ccc/"
	// strs := strings.Split(str, "/")
	// fmt.Printf("len %d\n", len(strs))
	// for i, iterm := range strs {
	// 	fmt.Printf("%d %s\n", i, iterm)
	// }

	// str := "aaa/bbb/ccc"
	// str = filepath.Join(str, "/")
	// fmt.Printf("str: %s,aaa", str)

	// a := make([]string, 0, 10)
	// fmt.Println(len(a))

	// rawUrl := "http://seaweedfs-filer.gemini-storage:8888/Gemini-Snapshot/traindata/asohtpaio6do/1/28088554866280448/latest/???????pretty=y&wantsMore=true"
	// escapUrl := url.QueryEscape(rawUrl)
	// fmt.Println(escapUrl)

	// path := "aaa"
	// a, b := filepath.Split(path)
	// fmt.Println(a, b)

	// if err := mygzip.Gzip("/home/zhangenshi/backup/test/go/myzip.gz", mygzip.DefaultCompression, mygzip.AES192, []byte(""), "/home/zhangenshi/backup/test/go/myzip/", "/home/zhangenshi/backup/test/go/mygzip/"); err != nil {
	// 	fmt.Println(err)
	// }

	if err := mygzip.UnGzip("/home/zhangenshi/backup/test/go/test/", "/home/zhangenshi/backup/test/go/myzip.gz", mygzip.AES192, []byte("")); err != nil {
		fmt.Println(err)
	}

	// hash := sha256.New()
	// if _, err := hash.Write([]byte("asadsadgjdsajgkhdkajshfkhakskgfgajsgjhskahkdhkjasas")); err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// fmt.Println(hash.Sum(nil))

	// err := os.Chtimes("/mnt/zes_test", time.UnixMilli(1642693492000), time.UnixMilli(1642693492000))
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// for i := 1; i < 5; i++ {
	// 	files, _, err := s3.QueryExternalDataSubdirectoryContents("https://obs.cn-east-3.myhuaweicloud.com",
	// 		"A42DBLYHYEOEGPUXGXRX", "yckRdqGa904eQTvnzdYVlZwfKOQljlj7a4WCyfCe", "cn-east-3", "eden", "aaaa", 5, i)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return
	// 	}
	// 	for _, file := range files {
	// 		fmt.Printf("%s\n", file.Name())
	// 	}
	// }
}
