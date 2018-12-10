cmake_minimum_required(VERSION 3.1.0)
project(rbd2qed)

set(CMAKE_VERBOSE_MAKEFILE ON)


set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(main main.cpp qed.cpp)
add_executable(test qed_main.cpp qed.cpp)

set_property(TARGET main PROPERTY CXX_STANDARD 11)

target_link_libraries(main Threads::Threads rbd rados stdc++)
target_link_libraries(test stdc++)
