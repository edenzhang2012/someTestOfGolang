package tools

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandomString returns a random string with a fixed length
func RandomString(n int, allowedChars ...[]rune) string {
	var letters []rune

	if len(allowedChars) == 0 {
		letters = defaultLetters
	} else {
		letters = allowedChars[0]
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

type PatchOp struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

type StructPatchBuilder struct {
	basePath string
	ops      []PatchOp
}

func NewStructPatchBuilder(basePath string) *StructPatchBuilder {
	return &StructPatchBuilder{
		basePath: basePath,
		ops:      make([]PatchOp, 0),
	}
}

func (b *StructPatchBuilder) UpdateByKey(
	parentPath []string,
	listField string,
	keyField string,
	keyValue string,
	targetPath []string,
	newValue interface{},
) *StructPatchBuilder {

	// 构造一个“占位 path”，index 在 Apply 阶段替换
	path := b.basePath

	for _, p := range parentPath {
		path += "/" + p
	}

	// 特殊语法：用 @key=value 表示“语义定位”
	path += fmt.Sprintf(
		"/%s/@%s=%s",
		listField,
		keyField,
		keyValue,
	)

	for _, p := range targetPath {
		path += "/" + p
	}

	b.ops = append(b.ops, PatchOp{
		Op:    "replace",
		Path:  path,
		Value: newValue,
	})

	return b
}

func (b *StructPatchBuilder) BuildWithObject(obj map[string]interface{}) ([]byte, error) {
	resolved := make([]PatchOp, 0, len(b.ops))

	for _, op := range b.ops {
		rp, err := resolveSemanticPath(op.Path, obj)
		if err != nil {
			return nil, err
		}
		op.Path = rp
		resolved = append(resolved, op)
	}

	return json.Marshal(resolved)
}

func resolveSemanticPath(path string, obj map[string]interface{}) (string, error) {
	segments := splitPath(path)

	cur := interface{}(obj)
	resolved := ""

	for i := 0; i < len(segments); i++ {
		seg := segments[i]

		// 语义选择器
		if isSemantic(seg) {
			key, val, err := parseSemantic(seg)
			if err != nil {
				return "", err
			}

			// 前一个 segment 一定是 list 名
			prev := segments[i-1]

			list, ok := cur.([]interface{})
			if !ok {
				return "", fmt.Errorf("field %s is not list", prev)
			}

			idx := -1
			for j, v := range list {
				m, ok := v.(map[string]interface{})
				if !ok {
					continue
				}

				// ingress 是 []string，需要 contains
				if arr, ok := m[key].([]interface{}); ok {
					for _, x := range arr {
						if x == val {
							idx = j
							cur = m
							break
						}
					}
				} else if m[key] == val {
					idx = j
					cur = m
				}

				if idx >= 0 {
					break
				}
			}

			if idx < 0 {
				return "", fmt.Errorf("no element with %s=%s", key, val)
			}

			// 替换上一个路径为 index
			resolved = strings.TrimSuffix(resolved, "/"+prev)
			resolved += fmt.Sprintf("/%s/%d", prev, idx)
			continue
		}

		// 普通字段
		resolved += "/" + seg

		switch c := cur.(type) {
		case map[string]interface{}:
			cur = c[seg]
		case []interface{}:
			// index
			idx, _ := strconv.Atoi(seg)
			cur = c[idx]
		}
	}

	return resolved, nil
}

func splitPath(p string) []string {
	out := make([]string, 0)
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func isSemantic(seg string) bool {
	return strings.HasPrefix(seg, "@")
}

func parseSemantic(seg string) (key, val string, err error) {
	// seg 形如 "@key=value"
	kv := strings.SplitN(seg[1:], "=", 2)
	if len(kv) != 2 {
		return "", "", fmt.Errorf("invalid semantic selector: %s", seg)
	}
	return kv[0], kv[1], nil
}
