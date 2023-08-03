cmake_minimum_required(VERSION 3.21)
project(indexSquare)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_DEPENDENCY /opt/dependency/package)

if (CMAKE_BUILD_TYPE)
    if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
        message(${CMAKE_BUILD_TYPE})
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
        set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
        message("========== Compile in asan mode ==========")
    endif()
endif()

include_directories(${PROJECT_SOURCE_DIR}/src)

enable_testing()
add_subdirectory(test)
