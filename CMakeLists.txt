cmake_minimum_required(VERSION 3.21)
project(art)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_DEPENDENCY /opt/dependency/package)

include_directories(${PROJECT_SOURCE_DIR}/src)

enable_testing()

add_subdirectory(test)
add_subdirectory(example)