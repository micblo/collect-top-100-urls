package main

import (
	"os"
	"strconv"
	"testing"
)

// go test -bench=. -benchmem -cpuprofile prof.cpu
func BenchmarkAddTagsToName(b *testing.B) {
	_ = os.Mkdir("./test", 0666)
	for i := 0; i < b.N; i++ {
		err := GenerateURLs("./urls.txt", "./test/"+strconv.Itoa(i), 1024*1024*10, false, 3)
		if err != nil {
			b.Error(err)
		}
	}

}
