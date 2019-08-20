package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func GenerateURLs(sourcePath, outputPath string, maxSize int) error {
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

	// Open output file
	fp, err := os.OpenFile(outputPath, os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("open file: %v error: %v", outputPath, err)
	}

	// Start generate
	size := 0
	now := time.Now()

	for ; size < maxSize; {
		n, err := fp.WriteString(urls[rand.Intn(len(urls))] + "\n")
		if err != nil {
			return fmt.Errorf("write file: %v error: %v", outputPath, err)
		}

		size += n
	}

	fmt.Printf("output path = %s\n", outputPath)
	fmt.Printf("max size = %d\n", maxSize)
	fmt.Printf("real size = %d\n", size)
	fmt.Printf("time = %v\n", time.Since(now))


	_ = fp.Close()

	return nil
}

func main()  {
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

	err = GenerateURLs(os.Args[1], os.Args[2], maxSize)
	if err != nil {
		fmt.Println(err)
		return
	}
}
