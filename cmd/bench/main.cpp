#include <iostream>
#include <typeinfo>

#include <usearch/index_dense.hpp>

#include "raft.grpc.pb.h"

int main() {
    // The bench stub stays intentionally small in T-01, but it still validates
    // that the generated gRPC header and USearch headers are available.
    using BootstrapService = raftvdb::proto::BootstrapService;
    using DenseIndex = unum::usearch::index_dense_t;

    std::cout << "RaftVDB bench bootstrap ready. "
              << "service_tag=" << typeid(BootstrapService).name() << ", "
              << "dense_index_size=" << sizeof(DenseIndex) << '\n';
    return 0;
}
