cmake_minimum_required(VERSION 3.16)
project(vdb)
set(CMAKE_C_STANDARD 99)
add_compile_options(-Wall -Wextra -pedantic -pthread -O2)
#add_definitions(-w) #hide all warnings for now to fix errors when converting to c++
add_link_options(-pthread)
add_subdirectory(src)
