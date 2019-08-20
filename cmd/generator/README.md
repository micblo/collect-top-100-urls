# URLs Generator

A generator which can generate a fixed-size file with many URLs.

## Test result

Device:
- CPU: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
- Disk: SKHynix_HFS256GD9TNG-L5B0B
- Go version: go1.12.6 windows/amd64

1 thread, 1GB File:

```
output path = result.txt
max size = 1073741824
real size = 1073741828
time = 5m6.4405502s
```

8 thread, 1GB File:

```
output path = result.txt
max size = 1073741824
real size = 1073741965
time = 1m16.9535918s
```

## Benchmark

```
go test -bench=. -benchmem -cpuprofile prof.cpu
go tool pprof stats.test  prof.cpu
```
