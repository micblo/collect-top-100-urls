# URLs Generator

A generator which can generate a fixed-size file with many URLs.

## Progress

1. use goroutine to generate files
2. merge files to one file 

## Test result

Device:
- CPU: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
- Disk: SKHynix_HFS256GD9TNG-L5B0B (Write: about 350MB/s)
- Go version: go1.12.6 windows/amd64

3 thread, 1GB File, merged:

```
output path = result.txt
max size = 1073741824
real size = 1073741874
time = 4.3124735s
```

3 thread, 100GB File, merged:

```
output path = result.txt
max size = 107374182400
real size = 107374182438
time = 17m59.5401807s
```

## Future

When the file generated is too big,
maybe it is faster to use goroutine to generate to one file.

## Benchmark

```
go test -bench=. -benchmem -cpuprofile prof.cpu
go tool pprof stats.test  prof.cpu
```
