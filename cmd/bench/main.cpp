#include <iostream>
#include <typeinfo>

#include <vector/index_dense.hpp>

#include "raft.grpc.pb.h"

int main() {
    // bench 入口暂时只做最小验证：
    // 1. gRPC 生成的 Service/Stub 类型可见
    // 2. vendoring 的 USearch 头文件可见
    using RaftService = raftvdb::proto::RaftService;
    using DenseIndex = unum::usearch::index_dense_t;

    std::cout << "RaftVDB bench bootstrap ready. "
              << "service_tag=" << typeid(RaftService).name() << ", "
              << "dense_index_size=" << sizeof(DenseIndex) << '\n';
    return 0;
}
