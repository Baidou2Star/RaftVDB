#include <iostream>

#include <vector/index_dense.hpp>

#include "raft.pb.h"

int main() {
    // 这里保留一个非常轻量的启动入口，
    // 先验证 vendoring 的 USearch 头文件和 protobuf 生成结果都能被正常链接。
    raftvdb::proto::Empty empty_message;
    raftvdb::proto::LeaderInfo leader_info;
    leader_info.set_leader_id("bootstrap");

    using DenseIndex = unum::usearch::index_dense_t;

    std::cout << "RaftVDB node bootstrap ready. "
              << "protobuf_bytes=" << empty_message.ByteSizeLong() << ", "
              << "leader_info_bytes=" << leader_info.ByteSizeLong() << ", "
              << "dense_index_size=" << sizeof(DenseIndex) << '\n';
    return 0;
}
