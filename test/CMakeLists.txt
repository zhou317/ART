file(GLOB_RECURSE test_source
        ${PROJECT_SOURCE_DIR}/src/*/*.h
        ${PROJECT_SOURCE_DIR}/src/*/*.cpp
        ${PROJECT_SOURCE_DIR}/test/*.cpp)

add_executable(indexTest ${test_source})

target_link_libraries(indexTest
        ${CMAKE_DEPENDENCY}/lib/libgtest.a
        ${CMAKE_DEPENDENCY}/lib/libgtest_main.a
        pthread)
