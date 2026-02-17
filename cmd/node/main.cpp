#include <iostream>

#include <vector/index_dense.hpp>

#include "raft.pb.h"

int main() {
    // Touch both the vendored USearch headers and the generated protobuf code
    // so the bootstrap target fails fast if the build wiring is incomplete.
    raftvdb::proto::Empty empty_message;

    using DenseIndex = unum::usearch::index_dense_t;

    std::cout << "RaftVDB node bootstrap ready. "
              << "protobuf_bytes=" << empty_message.ByteSizeLong() << ", "
              << "dense_index_size=" << sizeof(DenseIndex) << '\n';
    return 0;
}
