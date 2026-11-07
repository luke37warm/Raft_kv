cmake_minimum_required(VERSION 3.16)
project(raft_kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

if(WIN32)
  option(RAFT_SANITIZE "Build with AddressSanitizer + UBSan" OFF)
else()
  option(RAFT_SANITIZE "Build with AddressSanitizer + UBSan" ON)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)
if(RAFT_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

add_library(raft_core
  src/sim.cpp
  src/network.cpp
  src/storage.cpp
  src/node.cpp
)
target_include_directories(raft_core PUBLIC include)

enable_testing()
add_executable(test_foundations tests/test_foundations.cpp)
target_link_libraries(test_foundations PRIVATE raft_core)
add_test(NAME foundations COMMAND test_foundations)

add_executable(test_election tests/test_election.cpp)
target_link_libraries(test_election PRIVATE raft_core)
add_test(NAME election COMMAND test_election)
