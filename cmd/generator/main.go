package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func GenerateURLs(sourcePath, outputPath string, maxSize int, verbose bool) error {
	if maxSize <= 0 {
		return fmt.Errorf("Invalid max size of urls file. maxSize = %d\n", maxSize)
	}

	// Read URLs from sample file
	b, err := ioutil.ReadFile("./urls.txt")
	if err != nil {
		return fmt.Errorf("read file: %v error: %v", sourcePath, err)
	}

	s := string(b)
	urls := make([]string, 0)
	for _, lineStr := range strings.Split(s, "\n") {
		lineStr = strings.TrimSpace(lineStr)
		if lineStr == "" {
			continue
		}
		urls = append(urls, lineStr)
	}

	// Start generate
	size := 0
	now := time.Now()

	// 线程数
	childs := 8
	ch := make(chan int, childs)
	chErr := make(chan error)
	for i := 0; i < childs; i++ {
		go func(index, maxSize int) {
			// Open output file
			path := outputPath + "_" + strconv.Itoa(index)

			fp, err := os.OpenFile(path, os.O_CREATE, 0666)
			defer fp.Close()

			if err != nil {
				chErr <- fmt.Errorf("open file: %v error: %v", path, err)
				return
			}

			// Write
			size := 0
			w := bufio.NewWriter(fp)
			for size < maxSize {
				n, err := w.WriteString(urls[rand.Intn(len(urls))] + "\n")
				if err != nil {
					chErr <- fmt.Errorf("write file: %v error: %v", path, err)
					return
				}

				size += n
			}
			_ = w.Flush()

			ch <- size
		}(i, maxSize/childs)
	}

	for i := 0; i < childs; i++ {
		select {
		case s := <-ch:
			size += s
		case err = <-chErr:
			return err
		}
	}

	// Merge files
	out, err := os.OpenFile(outputPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatalln("failed to open first file for writing:", err)
	}
	defer out.Close()

	for i := 0; i < childs; i++ {
		path := outputPath + "_" + strconv.Itoa(i)
		in, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open file: %v error: %v", outputPath, err)
		}

		_, err = io.Copy(out, in)
		if err != nil {
			return fmt.Errorf("failed to append second file to first: %v\n", err)
		}

		in.Close()
		_ = os.Remove(path)
	}

	if verbose == true {
		fmt.Printf("output path = %s\n", outputPath)
		fmt.Printf("max size = %d\n", maxSize)
		fmt.Printf("real size = %d\n", size)
		fmt.Printf("time = %v\n", time.Since(now))
	}

	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("generator <urls-file> <output-file> <size>")
		return
	}

	reg := regexp.MustCompile(`^([0-9]+)([bkmg]|)$`)
	res := reg.FindAllStringSubmatch(os.Args[3], -1)
	if len(res) == 0 {
		fmt.Printf("maxSize must be a positive number with unit(b/k/m/g)!\n")
		return
	}

	maxSize, err := strconv.Atoi(res[0][1])
	if err != nil {
		fmt.Printf("maxSize must be a positive number!\n")
		return
	}

	switch res[0][2] {
	case "k":
		maxSize *= 1024
		break
	case "m":
		maxSize *= 1024 * 1024
		break
	case "g":
		maxSize *= 1024 * 1024 * 1024
		break
	}

	err = GenerateURLs(os.Args[1], os.Args[2], maxSize, true)
	if err != nil {
		fmt.Println(err)
		return
	}
}
