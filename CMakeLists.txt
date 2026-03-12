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
add_compile_definitions(
    USEARCH_USE_FP16LIB=0
    USEARCH_USE_SIMSIMD=0
)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(spdlog CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)

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
    src/common/logger.cpp
)
target_link_libraries(raftvdb_common
    PUBLIC
        spdlog::spdlog
        tomlplusplus::tomlplusplus
)

add_library(raftvdb_raft_log STATIC
    src/raft/log_entry.cpp
)
target_link_libraries(raftvdb_raft_log
    PUBLIC
        raftvdb_common
        raftvdb_proto
)

add_library(raftvdb_storage STATIC
    src/storage/wal.cpp
    src/storage/raft_meta.cpp
    src/storage/snapshot_store.cpp
)
target_link_libraries(raftvdb_storage
    PUBLIC
        raftvdb_common
        raftvdb_raft_log
        raftvdb_proto
)

add_library(raftvdb_vector STATIC
    src/vector/vector_index.cpp
    src/vector/index_maintenance.cpp
)

add_library(raftvdb_raft STATIC
    src/raft/raft_client.cpp
    src/raft/raft_server.cpp
    src/raft/raft_node.cpp
    src/raft/topology.cpp
    src/raft/lease.cpp
    src/client/dedup.cpp
    src/client/db_client.cpp
)
target_link_libraries(raftvdb_raft
    PUBLIC
        raftvdb_common
        raftvdb_proto
        raftvdb_raft_log
        raftvdb_storage
        raftvdb_vector
)

add_executable(raftvdb-node
    cmd/node/main.cpp
)
target_link_libraries(raftvdb-node
    PRIVATE
        raftvdb_common
        raftvdb_raft
        raftvdb_storage
        raftvdb_vector
        raftvdb_proto
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
        raftvdb_raft
        raftvdb_storage
        raftvdb_vector
        raftvdb_proto
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

    add_executable(raftvdb-test-logger
        tests/unit/test_logger.cpp
    )
    target_link_libraries(raftvdb-test-logger
        PRIVATE
            raftvdb_common
    )
    add_test(NAME raftvdb-test-logger COMMAND raftvdb-test-logger)

    add_executable(raftvdb-test-log-entry
        tests/unit/test_log_entry.cpp
    )
    target_link_libraries(raftvdb-test-log-entry
        PRIVATE
            raftvdb_raft_log
    )
    add_test(NAME raftvdb-test-log-entry COMMAND raftvdb-test-log-entry)

    add_executable(raftvdb-test-wal
        tests/unit/test_wal.cpp
    )
    target_link_libraries(raftvdb-test-wal
        PRIVATE
            raftvdb_storage
    )
    add_test(NAME raftvdb-test-wal COMMAND raftvdb-test-wal)

    add_executable(raftvdb-test-raft-meta
        tests/unit/test_raft_meta.cpp
    )
    target_link_libraries(raftvdb-test-raft-meta
        PRIVATE
            raftvdb_storage
    )
    add_test(NAME raftvdb-test-raft-meta COMMAND raftvdb-test-raft-meta)

    add_executable(raftvdb-test-snapshot-store
        tests/unit/test_snapshot_store.cpp
    )
    target_link_libraries(raftvdb-test-snapshot-store
        PRIVATE
            raftvdb_storage
    )
    add_test(NAME raftvdb-test-snapshot-store COMMAND raftvdb-test-snapshot-store)

    add_executable(raftvdb-test-vector-index
        tests/unit/test_vector_index.cpp
    )
    target_link_libraries(raftvdb-test-vector-index
        PRIVATE
            raftvdb_vector
    )
    add_test(NAME raftvdb-test-vector-index COMMAND raftvdb-test-vector-index)

    add_executable(raftvdb-test-vector-snapshot
        tests/unit/test_vector_snapshot.cpp
    )
    target_link_libraries(raftvdb-test-vector-snapshot
        PRIVATE
            raftvdb_storage
            raftvdb_vector
    )
    add_test(NAME raftvdb-test-vector-snapshot COMMAND raftvdb-test-vector-snapshot)

    add_executable(raftvdb-test-index-maintenance
        tests/unit/test_index_maintenance.cpp
    )
    target_link_libraries(raftvdb-test-index-maintenance
        PRIVATE
            raftvdb_vector
    )
    add_test(NAME raftvdb-test-index-maintenance COMMAND raftvdb-test-index-maintenance)

    add_executable(raftvdb-test-proto-smoke
        tests/unit/test_proto_smoke.cpp
    )
    target_link_libraries(raftvdb-test-proto-smoke
        PRIVATE
            raftvdb_proto
    )
    add_test(NAME raftvdb-test-proto-smoke COMMAND raftvdb-test-proto-smoke)

    add_executable(raftvdb-test-raft-client
        tests/unit/test_raft_client.cpp
    )
    target_link_libraries(raftvdb-test-raft-client
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-raft-client COMMAND raftvdb-test-raft-client)

    add_executable(raftvdb-test-db-client
        tests/unit/test_db_client.cpp
    )
    target_link_libraries(raftvdb-test-db-client
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-db-client COMMAND raftvdb-test-db-client)

    add_executable(raftvdb-test-raft-server
        tests/unit/test_raft_server.cpp
    )
    target_link_libraries(raftvdb-test-raft-server
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-raft-server COMMAND raftvdb-test-raft-server)

    add_executable(raftvdb-test-topology
        tests/unit/test_topology.cpp
    )
    target_link_libraries(raftvdb-test-topology
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-topology COMMAND raftvdb-test-topology)

    add_executable(raftvdb-test-lease
        tests/unit/test_lease.cpp
    )
    target_link_libraries(raftvdb-test-lease
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-lease COMMAND raftvdb-test-lease)

    add_executable(raftvdb-test-dedup
        tests/unit/test_dedup.cpp
    )
    target_link_libraries(raftvdb-test-dedup
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-dedup COMMAND raftvdb-test-dedup)

    add_executable(raftvdb-test-raft-node
        tests/unit/test_raft_node.cpp
    )
    target_link_libraries(raftvdb-test-raft-node
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-raft-node COMMAND raftvdb-test-raft-node)

    add_executable(raftvdb-test-raft-runtime
        tests/unit/test_raft_runtime.cpp
    )
    target_link_libraries(raftvdb-test-raft-runtime
        PRIVATE
            raftvdb_raft
    )
    add_test(NAME raftvdb-test-raft-runtime COMMAND raftvdb-test-raft-runtime)
endif()
