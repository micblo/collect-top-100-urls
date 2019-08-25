cmake_minimum_required(VERSION 3.14)
project(top100_url C)

set(CMAKE_C_STANDARD 11)

add_executable(top100_url main.c utils/utils.c utils/hashmap.c partition.c reduce.c utils/heap.c)