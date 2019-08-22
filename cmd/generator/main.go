package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("generator <urls-file> <output-file> <size> [children]")
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

	children := 4
	if len(os.Args) >= 5 {
		children, err = strconv.Atoi(os.Args[4])
		if err != nil {
			fmt.Printf("maxSize must be a positive number!\n")
			return
		}
	}

	err = GenerateURLs(os.Args[1], os.Args[2], maxSize, true, children)
	if err != nil {
		fmt.Println(err)
		return
	}
}
