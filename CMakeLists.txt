cmake_minimum_required(VERSION 3.25)

# Allow developers to rely on the local vcpkg installation without having to
# pass the toolchain path manually every time.
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    if(DEFINED ENV{CMAKE_TOOLCHAIN_FILE} AND NOT "$ENV{CMAKE_TOOLCHAIN_FILE}" STREQUAL "")
        set(CMAKE_TOOLCHAIN_FILE
            "$ENV{CMAKE_TOOLCHAIN_FILE}"
            CACHE FILEPATH "CMake toolchain file")
    elseif(DEFINED ENV{VCPKG_ROOT} AND NOT "$ENV{VCPKG_ROOT}" STREQUAL "")
        set(CMAKE_TOOLCHAIN_FILE
            "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
            CACHE FILEPATH "CMake toolchain file")
    endif()
endif()

project(RaftVDB VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${CMAKE_SOURCE_DIR}/include)
include(CTest)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(spdlog CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)

add_library(usearch INTERFACE)
target_include_directories(usearch INTERFACE
    ${CMAKE_SOURCE_DIR}/third_party
)
target_compile_definitions(usearch INTERFACE
    USEARCH_USE_FP16LIB=0
    USEARCH_USE_SIMSIMD=0
)

set(RAFTVDB_PROTO_FILE ${CMAKE_SOURCE_DIR}/proto/raft.proto)
set(RAFTVDB_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${RAFTVDB_GENERATED_DIR})

set(RAFTVDB_PROTO_SRCS
    ${RAFTVDB_GENERATED_DIR}/raft.pb.cc
    ${RAFTVDB_GENERATED_DIR}/raft.grpc.pb.cc
)
set(RAFTVDB_PROTO_HDRS
    ${RAFTVDB_GENERATED_DIR}/raft.pb.h
    ${RAFTVDB_GENERATED_DIR}/raft.grpc.pb.h
)

# Generate protobuf and gRPC sources during the build so future schema changes
# only need a normal CMake configure/build cycle.
add_custom_command(
    OUTPUT ${RAFTVDB_PROTO_SRCS} ${RAFTVDB_PROTO_HDRS}
    COMMAND $<TARGET_FILE:protobuf::protoc>
    ARGS
        --proto_path=${CMAKE_SOURCE_DIR}/proto
        --cpp_out=${RAFTVDB_GENERATED_DIR}
        --grpc_out=${RAFTVDB_GENERATED_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${RAFTVDB_PROTO_FILE}
    DEPENDS ${RAFTVDB_PROTO_FILE}
    VERBATIM
)

add_library(raftvdb_proto STATIC ${RAFTVDB_PROTO_SRCS} ${RAFTVDB_PROTO_HDRS})
target_include_directories(raftvdb_proto PUBLIC ${RAFTVDB_GENERATED_DIR})
target_link_libraries(raftvdb_proto
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

add_library(raftvdb_common STATIC
    src/common/config.cpp
)
target_link_libraries(raftvdb_common
    PUBLIC
        tomlplusplus::tomlplusplus
)

add_executable(raftvdb-node
    cmd/node/main.cpp
)
target_link_libraries(raftvdb-node
    PRIVATE
        raftvdb_common
        raftvdb_proto
        usearch
        Boost::system
        spdlog::spdlog
        tomlplusplus::tomlplusplus
)

add_executable(raftvdb-bench
    cmd/bench/main.cpp
)
target_link_libraries(raftvdb-bench
    PRIVATE
        raftvdb_common
        raftvdb_proto
        usearch
        Boost::system
        spdlog::spdlog
        tomlplusplus::tomlplusplus
)

if(BUILD_TESTING)
    add_executable(raftvdb-test-common
        tests/unit/test_result_defer.cpp
    )
    add_test(NAME raftvdb-test-common COMMAND raftvdb-test-common)

    add_executable(raftvdb-test-config
        tests/unit/test_config.cpp
    )
    target_link_libraries(raftvdb-test-config
        PRIVATE
            raftvdb_common
    )
    add_test(NAME raftvdb-test-config COMMAND raftvdb-test-config)
endif()
