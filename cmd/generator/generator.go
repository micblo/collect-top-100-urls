package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func GenerateURLs(sourcePath, outputPath string, maxSize int, verbose bool, children int) error {
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

	// count of goroutine
	if children <= 0 {
		return fmt.Errorf("count of children must be bigger than zero")
	}

	ch := make(chan int, children)
	chErr := make(chan error)
	for i := 0; i < children; i++ {
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
		}(i, maxSize/children)
	}

	for i := 0; i < children; i++ {
		select {
		case s := <-ch:
			size += s
		case err = <-chErr:
			return err
		}
	}

	// Merge files
	out, err := os.OpenFile(outputPath+"_0", os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open first file for writing: %v\n", err)
	}

	for i := 1; i < children; i++ {
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

	out.Close()

	_ = os.Remove(outputPath)

	err = os.Rename(outputPath+"_0", outputPath)
	if err != nil {
		return fmt.Errorf("failed to rename: %v\n", err)
	}

	if verbose == true {
		fmt.Printf("output path = %s\n", outputPath)
		fmt.Printf("max size = %d\n", maxSize)
		fmt.Printf("real size = %d\n", size)
		fmt.Printf("time = %v\n", time.Since(now))
	}

	return nil
}
