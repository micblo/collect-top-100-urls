package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"stat_url/cmd/collector/utils"
	"sync"
)

const (
	// Total: 900MB in-memory
	MaxBufferCnt  = 2
	MaxBufferSize = 1024 // 450 * 1024 * 1024 450MB
)

// Map Reduce:
// 1. Partition: Hash(URL) and put URL to bucket
// 2. calculate TOP 100 URLs in different buckets
// 3. merge TOP 100 results

func initTempBuffer(size int64) [][]byte {
	buffers := make([][]byte, MaxBufferCnt)

	rest := size
	for i := 0; i < MaxBufferCnt && rest > 0; i++ {
		n := int64(MaxBufferSize)
		if rest < MaxBufferSize {
			n = rest
		}

		buffers[i] = make([]byte, n)
		rest -= n
	}

	return buffers
}

func Partition(src, bucketPath string) error {
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat failed: %v", err)
	}

	if info.IsDir() {
		return fmt.Errorf("src file is not a file: %s", src)
	}

	// Size of URLs File
	size := info.Size()
	if size == 0 {
		return fmt.Errorf("src file is empty: %s", src)
	}

	// Open file for slice
	fp, err := os.Open(src)
	defer fp.Close()
	if err != nil {
		return fmt.Errorf("open failed: %v", err)
	}

	buffers := initTempBuffer(size)
	var brokenUrl []byte

	now := 0
	var mutex sync.Mutex

	for {
		n, err := fp.Read(buffers[now])
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read failed: %v", err)
		}

		mutex.Lock()

		// Partition URLs
		go func(n int, buf []byte) {
			defer mutex.Unlock()

			if brokenUrl != nil {
				next := bytes.IndexByte(buf, '\n')
				rest := buf[:next]
				buf = buf[next+1:]
				url := append(brokenUrl, rest...)

				fmt.Println(string(url))
			}

			if len(buf) == 0 {
				return
			}

			urls := bytes.Split(buf, []byte{'\n'})
			for i := 0; i < len(urls)-1; i++ {
				fmt.Println(string(urls[i]))
			}

			lastIndex := len(urls) - 1
			if buf[len(buf)-1] != '\n' {
				brokenUrl = urls[lastIndex]
			} else {
				fmt.Println(string(urls[lastIndex]))
				brokenUrl = nil
			}

		}(n, buffers[now])

		now = (now + 1) % MaxBufferCnt
	}

	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("collector <path> <tmp_path>")
		return
	}

	srcPath := os.Args[1]
	bucketPath := os.Args[2]

	// Create temporary directory
	if !utils.Exists(bucketPath) {
		err := os.MkdirAll(bucketPath, os.ModePerm)
		if err != nil {
			fmt.Printf("mdkir failed: %v", err)
			return
		}
	}

	// Partition URLs to different buckets
	err := Partition(srcPath, bucketPath)
	if err != nil {
		fmt.Printf("partition failed: %v", err)
		return
	}

	// TODO: calculate count of URLs in buckets

	// TODO: calculate Top 100 URLs by heap

	// TODO: merge heaps to calculate Top 100 URLs

}
