package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	extensionsv1alpha1 "github.com/alibaba/higress/v2/api/extensions/v1alpha1"
	higress "github.com/alibaba/higress/v2/client/pkg/clientset/versioned"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/wI2L/jsondiff"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/fsnotify.v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var (
	client    *http.Client
	Transport *http.Transport
)

func init() {
	Transport = &http.Transport{
		//DisableKeepAlives:   true, //关闭连接复用
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
	}
	client = &http.Client{
		Transport: Transport,
	}
}

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
	var ErrConflict = errors.New("geminifs lock conflict")
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
	if ctx == context.TODO() {
		res, err = client.Do(req)
	} else {
		res, err = client.Do(req.WithContext(ctx))
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

		if res.StatusCode == 409 {
			return nil, ErrConflict
		}

		if len(body) > 0 {
			fmt.Println("SimpleHttp get res: code ", res.StatusCode, " body ", string(body))
			if err := json.Unmarshal(body, &resp); err != nil {
				fmt.Println("SimpleHttp unmarshal failed with ", err)
				return nil, err
			}

			if resp.Err != "" {
				if strings.Contains(resp.Err, "already exist") {
					return nil, os.ErrExist
				}

				if strings.Contains(resp.Err, "not found") || strings.Contains(resp.Err, "not exist") || strings.Contains(resp.Err, "is already stopped") /*在线同步取消场景*/ {
					return nil, os.ErrNotExist
				}

				fmt.Printf("SimpleHttp failed with: %s\n", resp.Err)
				return nil, fmt.Errorf("SimpleHttp failed with: %s", resp.Err)
			}
		} else {
			return nil, fmt.Errorf("SimpleHttp failed with http status %d and empty body", res.StatusCode)
		}
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
	for i := 0; i < 100; i++ {
		params := url.Values{}
		params.Add("startFrom", strconv.Itoa(1))
		params.Add("limit", strconv.Itoa(100))
		rawUrl := "http://10.244.40.236:8888/Gemini-Snapshot/codeset/wfmvnrnit4up/1/393035500987879424/latest"
		rawUrl = rawUrl + "?" + params.Encode()
		go func() {
			body, err := SimpleHttp(context.TODO(), "GET", rawUrl, nil, map[string]string{"Accept": "application/json"})
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(string(body))
		}()
	}

	for i := 1; i < 7; i++ {
		str := fmt.Sprintf("http://10.244.40.236:8888/Gemini-Snapshot/codeset/wfmvnrnit4up/1/393035500987879424/latest/%d.txt", i)
		go func() {
			body, err := SimpleHttp(context.TODO(), "DELETE", str, nil, nil)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(string(body))
		}()
	}

	time.Sleep(100 * time.Second)

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

	// rawUrl := filerPath("/t123H就sbajd%jsahh.jpg")
	// params := url.Values{}
	// params.Add("metadata", "true")
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// body, err := SimpleHttp(ctx, "GET", rawUrl, &params, nil)
	// if err != nil {
	// 	if err == context.DeadlineExceeded {
	// 		fmt.Println("aaaaaaa")
	// 	}

	// 	if errors.Is(err, context.DeadlineExceeded) {
	// 		fmt.Println("bbbbbbb")
	// 	}

	// 	if err, ok := err.(*url.Error); ok && err.Timeout() {
	// 		fmt.Println("ccccc")
	// 	}
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Println(string(body))
	// }
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

func TurnToBashString(src string) string {
	fmt.Printf("##### get src %s\n", src)
	if strings.Contains(src, `'`) || strings.Contains(src, `\`) {
		src = strings.ReplaceAll(src, `\`, `\\`)
		src = strings.ReplaceAll(src, `'`, `\'`)
		src = fmt.Sprintf("$'%s'", src)
	} else {
		src = fmt.Sprintf("'%s'", src)
	}
	fmt.Printf("##### return src %s\n", src)
	return src
}

func workerA(num int, ch chan struct{}, wg *sync.WaitGroup) {
	fmt.Println(num, "start")
	sleepTime := num/3 + 1
	time.Sleep(time.Duration(sleepTime))
	fmt.Println(num, "end")
	ch <- struct{}{}
	wg.Done()
}

func dealWithOfflineSyncData(worker func(int, chan struct{}, *sync.WaitGroup), maxThreads, total int) {
	if maxThreads >= total {
		maxThreads = total
	}

	wg := sync.WaitGroup{}
	wg.Add(total)
	ch := make(chan struct{}, maxThreads)
	processed := 0
	done := 0

	for i := 0; i < maxThreads; i++ {
		processed++
		go worker(i, ch, &wg)
	}

	for {
		<-ch
		done++

		if processed >= total {
			break
		}

		processed++
		go worker(maxThreads+done-1, ch, &wg)
	}

	wg.Wait()
}

func TurnOldRealPathToLogicalPath(path string) (string, string, error) {
	hasTail := false
	tmpStr := strings.TrimPrefix(path, "/pavostor")
	tmpStr = strings.TrimPrefix(tmpStr, "/gemini/")
	if strings.HasSuffix(tmpStr, "/") {
		hasTail = true
		tmpStr = strings.TrimSuffix(tmpStr, "/")
	}

	strs := strings.Split(tmpStr, "/")

	logicalPath := ""
	suffix := ""
	switch strs[0] {
	case "codeset":
		if len(strs) < 4 {
			return "", "", fmt.Errorf("path %s is invalid", path)
		}
		logicalPath = filepath.Join(strs[0], strs[1], "padding", strs[2], strs[3])
		if hasTail && len(strs) > 4 {
			suffix = filepath.Join(strs[4:]...) + "/"
		} else {
			suffix = filepath.Join(strs[4:]...)
		}
		return logicalPath, suffix, nil
	case "model", "output", "traindata":
		if len(strs) < 3 {
			return "", "", fmt.Errorf("path %s is invalid", path)
		}
		logicalPath = filepath.Join(strs[0], strs[1], "padding", strs[2], "padding")
		if hasTail && len(strs) > 3 {
			suffix = filepath.Join(strs[3:]...) + "/"
		} else {
			suffix = filepath.Join(strs[3:]...)
		}
		return logicalPath, suffix, nil
	default:
		return "", "", fmt.Errorf("type %s, path %s is invalid", strs[0], path)
	}
}

func lengthOfLongestSubstring(s string) int {
	/*
		常规思路：
		在主串上进行遍历，并将不重复的子串字母放入map中，子串的尾index在主串上递增，
		一直到子串的尾index的值在map中存在，记录下map的长度。将子串的头index累加后继续，
		一直到子串的尾index等于主串的尾index
	*/
	sLen := len(s)
	subStringHeadIndex := 0
	subStringTailIndex := subStringHeadIndex + 1
	maxLen := 1
	for ; subStringTailIndex < sLen; subStringTailIndex++ {
		if index := strings.IndexByte(s[subStringHeadIndex:subStringTailIndex],
			s[subStringTailIndex]); index >= 0 {
			if maxLen < len(s[subStringHeadIndex:subStringTailIndex]) {
				maxLen = len(s[subStringHeadIndex:subStringTailIndex])
			}
			subStringHeadIndex++
			subStringTailIndex = subStringHeadIndex
		}
	}

	if maxLen < len(s[subStringHeadIndex:subStringTailIndex]) {
		maxLen = len(s[subStringHeadIndex:subStringTailIndex])
	}

	return maxLen
}

func findMedianSortedArrays(nums1 []int, nums2 []int) float64 {
	/*
		   解法1：最通俗的解法，先合并，再找中位数
		   空间复杂度:O(m+n)
		   时间复杂度:O(m+n)
		   BUG：
		   1. 嵌套遍历不对，因为不知道哪个数组先遍历完，改为单层遍历
		   2. golang数字精度问题，整数除浮点数会得到整数，需要在除之前就进行转换
		   结果：
		   Your runtime beats 100 % of golang submissions
			Your memory usage beats 6.39 % of golang submissions (6.6 MB)
	*/
	// total := make([]int, 0, len(nums1)+len(nums2))
	// i, j := 0, 0
	// for i < len(nums1) && j < len(nums2) {
	// 	if nums1[i] < nums2[j] {
	// 		total = append(total, nums1[i])
	// 		i++
	// 	} else {
	// 		total = append(total, nums2[j])
	// 		j++
	// 	}
	// }

	// if i != len(nums1) {
	// 	for ; i < len(nums1); i++ {
	// 		total = append(total, nums1[i])
	// 	}
	// }

	// if j != len(nums2) {
	// 	for ; j < len(nums2); j++ {
	// 		total = append(total, nums2[j])
	// 	}
	// }

	// if len(total)%2 == 0 {
	// 	return float64((total[len(total)/2] + total[len(total)/2-1])) / 2.0
	// } else {
	// 	return float64(total[len(total)/2])
	// }

	/*
		解法2：中位数，两个数组是有序的，两个数组长度都知道，中位数的位置也可以知道，不需要合并，
		直接找到对应位置的数，计算即可
		时间复杂度：O(M+N)
		空间复杂度：O(1)
		BUG：
		1. 没有考虑当其中一个数据被先遍历完的边界场景
		2. 粗心

	*/
	total := len(nums1) + len(nums2)
	middle1 := total/2 - 1
	middle2 := total / 2
	middle1value, middle2Value, index := 0, 0, 0
	i, j := 0, 0

	for index <= middle2 && i < len(nums1) && j < len(nums2) {
		if nums1[i] < nums2[j] {
			if index == middle2 {
				middle2Value = nums1[i]
				break
			}

			if total%2 == 0 && index == middle1 {
				middle1value = nums1[i]
			}
			i++
		} else {
			if index == middle2 {
				middle2Value = nums2[j]
				break
			}

			if total%2 == 0 && index == middle1 {
				middle1value = nums2[j]
			}
			j++
		}
		index++
	}

	if i == len(nums1) {
		for ; j < len(nums2); j++ {
			if index == middle2 {
				middle2Value = nums2[j]
				break
			}

			if total%2 == 0 && index == middle1 {
				middle1value = nums2[j]
			}
			index++
		}
	}

	if j == len(nums2) {
		for ; i < len(nums1); i++ {
			if index == middle2 {
				middle2Value = nums1[i]
				break
			}

			if total%2 == 0 && index == middle1 {
				middle1value = nums1[i]
			}
			index++
		}
	}

	if total%2 == 0 {
		return float64(middle1value+middle2Value) / 2.0
	} else {
		return float64(middle2Value)
	}
}

func aaaInit(path string) error {
	return nil
}

func aaaHandler(event fsnotify.Event) error {
	fmt.Println(event)
	return nil
}

type QuotaCheckConfig struct {
	RunMode   int    `json:"run_mode"`   //0:运行并记录数据库，默认值；1：只检查元数据，并输出不一致的数据信息，不记录数据库
	RunType   int    `json:"run_type"`   //0:创建定时任务，默认值;1：立即运行一遍，若定时任务正在运行中，则不运行
	Shutdown  int    `json:"shutdown"`   //0：不做任何动作，默认值；1：立即停止正在进行的normal任务，如果是定时任务，则一并取消定时任务
	CronSpec  string `json:"cron_spec"`  //若RunType为timed,该值指定cron规则，默认为"0 1 * * *"每天凌晨1点执行一次
	LatestNum int    `json:"latest_num"` //指定检查的最新快照的数量，默认100
	RandomNum int    `json:"random_num"` //指定检查的随机快照数量，默认100
}

func blockPadding(offset int64) (n int64) {
	return -offset & (512 - 1)
}

func convert(s string, numRows int) string {
	n := len(s)
	if n <= numRows || numRows == 1 {
		return s
	}

	/*
		方案1：模拟
		建立二维数组，将字符串的值按照题设方法依次填进数组，再从左到右扫描，跳过为空的部分
		两个难点：
		1. 如何确定二维数组的列数（go语言里利用slice可以规避这个问题）
			按照规律将数图切成 V 字形，从第1个顶点到第2个顶点之间（不含第二个顶点）的字母个数为
			r+r-2。N 字形可以拆成多个这样的 V 字形。每个 V 字形的的列数为 r-1。len(s)/(2r-2)
			可得到V字形个数，len(s)%(2r-2)得到余数，余数<=r 则总列数+1，否则总列数+(余数-r+1)
		2. 填充时如何确定数据对应在二维数组中的位置
			遍历数组，在对应位置填值
	*/

	// a := n / (2*numRows - 2)
	// b := n % (2*numRows - 2)
	// c := 0
	// if b > 0 && b <= numRows {
	// 	c = 1
	// } else if b > numRows {
	// 	c = b - numRows + 1
	// }

	// x := a*(numRows-1) + c

	// tmp := make([][]byte, numRows)
	// for i := 0; i < numRows; i++ { //初始化二维数组
	// 	tmp[i] = make([]byte, x)
	// }

	// for j, index := 0, 0; j < x; {
	// 	if j%(numRows-1) == 0 { //需要填充整列
	// 		for i := 0; i < numRows; i++ {
	// 			if index < n {
	// 				tmp[i][j] = s[index]
	// 				index++
	// 			} else {
	// 				break
	// 			}
	// 		}
	// 		j++
	// 	} else {
	// 		for i := numRows - 2; i > 0; i-- {
	// 			if index < n {
	// 				tmp[i][j] = s[index]
	// 				index++
	// 				j++
	// 			} else {
	// 				j++
	// 				break
	// 			}
	// 		}
	// 	}
	// }

	// ret := make([]byte, n)
	// for i, index := 0, 0; i < numRows; i++ {
	// 	for j := 0; j < x; j++ {
	// 		if tmp[i][j] != 0 {
	// 			ret[index] = tmp[i][j]
	// 		}
	// 	}
	// }

	// return string(ret)

	T := 2*numRows - 2 //周期
	tMax := 0
	if n%T == 0 {
		tMax = n / T
	} else {
		tMax = n/T + 1
	}

	ret := make([]byte, n)
	index := 0
	for r := 0; r < numRows; r++ { //遍历行
		for t := 0; t < tMax; t++ { //遍历周期
			ret[index] = s[t*T+r] //第一个数字
			index++
			if r > 0 && r < numRows-1 && (t+1)*T-r < n {
				ret[index] = s[(t+1)*T-r]
				index++
			}
		}
	}

	return string(ret)
}

func privateKeyFromFile(file string) []byte {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("ReadFile %s failed with %v\n", file, err)
		// 这里可以根据需要进行错误处理，比如返回错误或终止程序
		return nil
	}

	fmt.Println(string(bytes))
	return bytes
}

func getClient() (*higress.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		fmt.Printf("rest.InClusterConfig error %v\n", err)
		home := homedir.HomeDir()
		if home != "" {
			configPath := filepath.Join(home, ".kube", "config")
			config, err := clientcmd.BuildConfigFromFlags("", configPath)
			if err == nil {
				fmt.Printf("rest.InClusterConfig QPS %f, Burst %d\n", config.QPS, config.Burst)

				higressClientset, err := higress.NewForConfig(config)
				if err != nil {
					return nil, err
				}

				return higressClientset, nil
			}
		}

		return nil, err
	}
	config.QPS = 50
	config.Burst = 100

	higressClientset, err := higress.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return higressClientset, nil
}

func main() {
	// logical, suffix, err := TurnOldRealPathToLogicalPath("/gemini/codeset/y160hxjsfe5y/486804894558007296/latest/occgkstpzsrhnraocxev")
	// fmt.Println(logical, suffix, err)

	// logical, suffix, err = TurnOldRealPathToLogicalPath("/gemini/traindata/y160hxjsfe5y/486804894558007296/occgkstpzsrhnraocxev")
	// fmt.Println(logical, suffix, err)

	// logical, suffix, err = TurnOldRealPathToLogicalPath("/gemini/codeset/y160hxjsfe5y/486804894558007296/latest")
	// fmt.Println(logical, suffix, err)

	// logical, suffix, err = TurnOldRealPathToLogicalPath("/gemini/traindata/y160hxjsfe5y/486804894558007296")
	// fmt.Println(logical, suffix, err)

	// logical, suffix, err = TurnOldRealPathToLogicalPath("/gemini/codeset/y160hxjsfe5y/486804894558007296/latest/")
	// fmt.Println(logical, suffix, err)

	// logical, suffix, err = TurnOldRealPathToLogicalPath("/gemini/traindata/y160hxjsfe5y/486804894558007296/")
	// fmt.Println(logical, suffix, err)

	// logical, suffix, err = TurnOldRealPathToLogicalPath("/gemini/codeset/y160hxjsfe5y/486804894558007296/latest/occgkstpzsrhnraocxev/a/b/c/")
	// fmt.Println(logical, suffix, err)

	// logical, suffix, err = TurnOldRealPathToLogicalPath("/gemini/traindata/y160hxjsfe5y/486804894558007296/occgkstpzsrhnraocxev/a/b/c/")
	// fmt.Println(logical, suffix, err)
	// return
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

	// if err := mygzip.UnGzip("/home/zhangenshi/backup/test/go/test/", "/home/zhangenshi/backup/test/go/myzip.gz", mygzip.AES192, []byte("")); err != nil {
	// 	fmt.Println(err)
	// }

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

	// fmt.Println(strings.TrimSpace(""))

	// poll, err := threadpool.NewThreadPool()
	// if err != nil {
	// 	fmt.Printf("create thread pool falied with %v\n", err)
	// }
	// fmt.Printf("poll: %s\n", poll.Describe())
	// poll.Stop()

	// cmd := TurnToBashString(`123'12\34`)
	// fmt.Println(cmd)
	// cmd = fmt.Sprintf("%s -C %s", cmd, "aaa")
	// fmt.Println(cmd)

	// dsn := "mongodb://root:password@10.12.10.56:37017/?authMechanism=SCRAM-SHA-1&authSource=admin&directConnection=true"
	// mongodb.InitMongodb(dsn, 0)

	// if err := mongodb.Index("pavostor", "task", []mongo.IndexModel{
	// 	{
	// 		Keys:    bson.M{"expire": 1}, //1表示升序，-1表示降序
	// 		Options: options.Index().SetExpireAfterSeconds(0),
	// 	}}); err != nil {
	// 	fmt.Println("error: ", err)
	// 	return
	// }

	// task := mongodb.Task{
	// 	TaskId: "aaaaaaaaaa",
	// 	Expire: time.Now().Add(30 * time.Second),
	// }
	// if err := task.Insert(); err != nil {
	// 	fmt.Println("error: ", err)
	// 	return
	// }

	// for i := 0; i < 10; i++ {
	// 	time.Sleep(20 * time.Second)
	// 	task.Expire = time.Now().Add(30 * time.Second)
	// 	if err := task.Update(); err != nil {
	// 		fmt.Println("error: ", err)
	// 		return
	// 	}
	// }
	// tmpTime := time.Now()
	// tmpTimeStr := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", tmpTime.Year(), tmpTime.Month(), tmpTime.Day(), tmpTime.Hour(), tmpTime.Minute(), tmpTime.Second())
	// fmt.Println(tmpTimeStr)
	// DirSize()

	// err := os.MkdirAll("aaa", os.ModePerm)
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Println("success")
	// }

	// file, err := os.OpenFile("aaa/a.txt", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Println("success")
	// 	file.Close()
	// }

	// dealWithOfflineSyncData(workerA, 8, 12)

	// ret := lengthOfLongestSubstring("bbbbb")
	// fmt.Println(ret)

	// if err := filewatch.StartFileWatcher(); err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// if err := filewatch.AddFileToWatch("/tmp/aaa", aaaInit, aaaHandler); err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// filewatch.ShutdownFileWatcher()

	// tmp := QuotaCheckConfig{
	// 	RunMode:   1,
	// 	RunType:   1,
	// 	Shutdown:  0,
	// 	CronSpec:  "0 1 * * *",
	// 	LatestNum: 100,
	// 	RandomNum: 100,
	// }

	// data, err := json.Marshal(tmp)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(string(data))

	// fmt.Println(findMedianSortedArrays([]int{1, 2, 3, 4, 5}, []int{6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}))

	// tmp := make([]byte, 100)
	// fmt.Println(len(tmp), cap(tmp))

	// tmp1 := tmp[:20]
	// fmt.Println(len(tmp1), cap(tmp1))

	// tmp2 := tmp[90:]
	// fmt.Println(len(tmp2), cap(tmp2))

	// data := make([]byte, 0, 100)
	// fmt.Println(len(data), cap(data))

	// data1 := data[:20]
	// fmt.Println(len(data1), cap(data1))

	// data2 := data[90:]
	// fmt.Println(len(data2), cap(data2))

	// fmt.Println(blockPadding(10256))

	// fmt.Println(convert("PAYPALISHIRING", 3))

	// if err := agent.Listen(agent.Options{Addr: "0.0.0.0:7777"}); err != nil {
	// 	log.Fatal(err)
	// }
	// total := 0
	// for i := 0; i < 100000000; i++ {
	// 	time.Sleep(200 * time.Microsecond)
	// 	total++
	// 	// fmt.Println(total)
	// }
	// select {}

	// 公钥文件路径
	// publicKeyPath := "/path/to/your/public_key.pub"
	// 私钥文件路径
	// privateKeyPath := "/home/zhangenshi/.ssh/id_rsa"

	// // 读取私钥
	// privateKeyBytes, err := ssh.ParsePrivateKey(privateKeyFromFile(privateKeyPath))
	// if err != nil {
	// 	log.Fatalf("Failed to parse private key: %v", err)
	// }

	// // 配置 SSH 客户端
	// config := &ssh.ClientConfig{
	// 	User: "root",
	// 	Auth: []ssh.AuthMethod{
	// 		ssh.PublicKeys(privateKeyBytes),
	// 	},
	// 	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	// }

	// // 连接的目标节点地址和端口
	// host := "10.12.10.51"
	// port := 22

	// // 连接到远程节点
	// client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", host, port), config)
	// if err != nil {
	// 	log.Fatalf("Failed to dial: %v", err)
	// }
	// defer client.Close()

	// // 创建一个会话
	// session, err := client.NewSession()
	// if err != nil {
	// 	log.Fatalf("Failed to create session: %v", err)
	// }
	// defer session.Close()

	// // 执行的 SSH 命令
	// command := "ls -l"
	// var stdout bytes.Buffer
	// session.Stdout = &stdout
	// err = session.Run(command)
	// if err != nil {
	// 	log.Fatalf("Failed to run command: %v", err)
	// }

	// // 输出命令执行结果
	// fmt.Println(stdout.String())

	// file, err := os.OpenFile("/home/zhangenshi/zes_test/mnt/aaaaaa", os.O_RDWR, os.ModePerm)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// n, err := file.WriteAt([]byte("y'y"), 2)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// fmt.Println(n)

	/*
		test jsonpatch
	*/
	higressClient, err := getClient()
	if err != nil {
		fmt.Printf("getClient failed: %v\n", err)
		return
	}

	targetStruct := &Config{
		Redis: RedisConfig{
			ServiceName: "redis.default.svc.cluster.local",
			ServicePort: 6379,
		},
		RuleName:             "default_rule",
		ShowLimitQuotaHeader: true,
		RuleItems: []RuleItem{
			{
				LimitByHeader: "Authorization",
				LimitKeys: []LimitKey{
					{
						Key:            "ccc",
						QueryPerMinute: 11,
					},
				},
			},
		},
	}
	target, err := json.Marshal(targetStruct)
	if err != nil {
		fmt.Printf("Marshal targetStruct failed: %v\n", err)
		return
	}
	fmt.Printf("new: %s\n", target)

	keyRateLimitConf, err := higressClient.ExtensionsV1alpha1().WasmPlugins("higress-system").Get(context.Background(), "cluster-key-rate-limit-1.0.0", v1.GetOptions{})
	if err != nil {
		fmt.Printf("get keyRateLimitConf failed: %v\n", err)
		return
	}

	var matchRule *extensionsv1alpha1.MatchRule
	index := -1
	if len(keyRateLimitConf.Spec.MatchRules) > 0 {
		for i := 0; i < len(keyRateLimitConf.Spec.MatchRules); i++ {
			if len(keyRateLimitConf.Spec.MatchRules[i].Ingress) == 1 && keyRateLimitConf.Spec.MatchRules[i].Ingress[0] == "openai-mock1" {
				matchRule = keyRateLimitConf.Spec.MatchRules[i]
				index = i
				break
			}
		}
	}
	if matchRule == nil {
		fmt.Printf("not found\n")
		//add item
		var config structpb.Struct
		err := protojson.Unmarshal(target, &config)
		if err != nil {
			fmt.Printf("Unmarshal target to structpb.Struct failed: %v\n", err)
			return
		}

		matchRule := &extensionsv1alpha1.MatchRule{
			Ingress:       []string{"openai-mock1"},
			ConfigDisable: &wrappers.BoolValue{Value: false},
			Config:        &config,
		}
		josnStr, err := json.Marshal(matchRule)
		if err != nil {
			fmt.Printf("marshal matchRule failed: %v\n", err)
			return
		}
		patch := jsondiff.Patch{
			jsondiff.Operation{
				Type:  jsondiff.OperationAdd,
				Path:  "/spec/matchRules/-",
				Value: josnStr,
			},
		}
		patchStr, err := json.Marshal(patch)
		if err != nil {
			fmt.Printf("Marshal patch failed: %v\n", err)
			return
		}
		fmt.Printf("patch: %s\n", patchStr)
		return
	}

	//test
	// old, err := json.Marshal(keyRateLimitConf)
	// if err != nil {
	// 	fmt.Printf("Marshal keyRateLimitConf failed: %v\n", err)
	// 	return
	// }
	// var config structpb.Struct
	// err = protojson.Unmarshal(target, &config)
	// if err != nil {
	// 	fmt.Printf("Unmarshal target to structpb.Struct failed: %v\n", err)
	// 	return
	// }
	// matchRule.Config = &config
	// new, err := json.Marshal(keyRateLimitConf)
	// if err != nil {
	// 	fmt.Printf("Marshal keyRateLimitConf failed: %v\n", err)
	// 	return
	// }
	// patch, err := jsondiff.CompareJSON(old, new)
	// if err != nil {
	// 	panic(err)
	// }
	// patchStr, err := json.Marshal(patch)
	// if err != nil {
	// 	fmt.Printf("Marshal patch failed: %v", err)
	// 	return
	// }
	// fmt.Printf("patch: %s\n", patchStr)

	//delete item
	// patch := jsondiff.Patch{
	// 	jsondiff.Operation{
	// 		Type: jsondiff.OperationRemove,
	// 		Path: fmt.Sprintf("/spec/matchRules/%d", index),
	// 	},
	// }
	// patchStr, err := json.Marshal(patch)
	// if err != nil {
	// 	fmt.Printf("Marshal patch failed: %v\n", err)
	// 	return
	// }
	// fmt.Printf("patch: %s\n", patchStr)
	// return

	//update
	old, err := json.Marshal(matchRule.Config)
	if err != nil {
		fmt.Printf("Marshal matchRule failed: %v", err)
		return
	}
	fmt.Printf("old: %s\n", old)

	patch, err := jsondiff.CompareJSON(old, target)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(patch); i++ {
		patch[i].Path = fmt.Sprintf("/spec/matchRules/%d/config%s", index, patch[i].Path)
	}
	patchStr, err := json.Marshal(patch)
	if err != nil {
		fmt.Printf("Marshal patch failed: %v", err)
		return
	}
	fmt.Printf("patch: %s\n", patchStr)

	_, err = higressClient.ExtensionsV1alpha1().WasmPlugins("higress-system").Patch(context.Background(), "cluster-key-rate-limit-1.0.0", types.JSONPatchType, []byte(patchStr), v1.PatchOptions{})
	if err != nil {
		fmt.Printf("Patch failed: %v\n", err)
		return
	}
	fmt.Println("Patch success!")
}

type Config struct {
	Redis                RedisConfig `json:"redis"`
	RuleItems            []RuleItem  `json:"rule_items"`
	RuleName             string      `json:"rule_name"`
	ShowLimitQuotaHeader bool        `json:"show_limit_quota_header"`
}

type RedisConfig struct {
	ServiceName string `json:"service_name"`
	ServicePort int    `json:"service_port"`
}

type RuleItem struct {
	LimitByHeader string     `json:"limit_by_header"`
	LimitKeys     []LimitKey `json:"limit_keys"`
}

type LimitKey struct {
	Key            string `json:"key"`
	QueryPerMinute int    `json:"query_per_minute"`
}
