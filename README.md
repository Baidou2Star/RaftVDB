# RaftVDB

> 基于 Raft 共识算法从零实现的分布式向量数据库，支持链式复制拓扑与租约读，底层向量引擎通过 HNSW 实现。
>
> 本项目为个人学习项目，旨在深入理解分布式系统、共识算法与向量检索引擎的工程实现。

---

## ✨ 核心特性

- 🗳️ **Raft 共识**：完整实现 Leader 选举、日志复制、日志压缩（快照）
- 🔗 **链式复制拓扑**：N=5 时引入 Mentor 层转发，Leader 出口带宽减半
- ⚡ **租约读（线性一致性）**：租约有效期内读请求零额外网络往返
- 🔍 **向量状态机**：基于 HNSW 的 ANN 检索，支持 Upsert、Delete、Search
- 🚀 **Pipeline + Batch 复制**：可配置在途窗口与批量大小，提升写入吞吐
- 📸 **无停顿快照**：后台线程保存，写路径不阻塞，原子 rename 保证崩溃安全
- 🔁 **幂等写入**：客户端 `request_id` 滑动窗口去重，网络重试不产生重复数据
- 🧭 **自动寻主**：客户端并发探测所有节点，Leader 变更时单跳重定向
- 🖥️ **单节点兼容**：配置文件仅含自身节点时自动退化为单节点模式

---

## 🏗️ 系统架构

```
┌─────────────────────────────────────────┐
│              客户端层                    │
│   DBClient  (Upsert / Delete / Search)  │
│   DedupTable  (request_id 幂等去重)      │
└──────────────────┬──────────────────────┘
                   │ gRPC
┌──────────────────▼──────────────────────┐
│               Raft 层                   │
│  RaftNode  ─  TopologyManager           │
│  LeaseManager  ─  WAL  ─  RaftMeta      │
│  RaftServer / RaftClient  (gRPC)        │
└──────────────────┬──────────────────────┘
                   │ Apply
┌──────────────────▼──────────────────────┐
│              向量层                      │
│  VectorIndex  (Upsert、Delete、Search)   │
│  IndexMaintenance  (compact / isolate)  │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│              存储层                      │
│  WAL  (CRC32 校验 + 段文件滚动)           │
│  SnapshotStore  (原子 rename 提交)        │
└─────────────────────────────────────────┘
```

### 链式复制拓扑（N=5）

```
Leader
├── Mentor A ──▶ Follower C
└── Mentor B ──▶ Follower D
```

- Leader 只向 Mentor 复制，出口带宽减半
- Follower ACK 直接回 Leader，不经过 Mentor
- Mentor 超时时 Leader 主动探测其下游 Follower，健康则提升为新 Mentor
- 支持 Mentor 扣压诊断（链路阻塞 vs 处理能力受限），分别采用不同恢复策略

---

## 📊 压测基线

本机3节点，`dim=1024`，4 线程，10,000 请求（2,000 预热）：

| 模式   | 吞吐          | P50     | P99      | P999     | 读超时率 |
| ------ | ------------- | ------- | -------- | -------- | -------- |
| upsert | 141.6 ops/sec | 29.3 ms | 55.3 ms  | 100.1 ms | 0%       |
| delete | 595.0 ops/sec | 6.2 ms  | 8.0 ms   | 14.0 ms  | 0%       |
| search | 908.7 ops/sec | 4.3 ms  | 5.8 ms   | 8.2 ms   | 0%       |
| mixed  | 72.3 ops/sec  | 23.8 ms | 506.6 ms | 507.4 ms | 1.55%    |
| full   | 67.6 ops/sec  | 23.6 ms | 508.5 ms | 509.6 ms | 0.73%    |

**说明：**
- `upsert` 瓶颈在 WAL `fdatasync` + HNSW 图插入 + Apply 串行路径
- `delete` 比 `upsert` 快约 4 倍：只需标记删除并写 WAL，无需更新 HNSW 图结构
- `search` 直接命中本地 HNSW 索引（租约读），无磁盘 I/O
- `mixed` / `full` 的读超时来自线性一致性读等待 Apply 追平，写入失败率全程为 0%
- 全程压测期间无非预期重新选举事件

---

## 🛠️ 技术选型

| 组件     | 选型            |
| -------- | --------------- |
| 共识算法 | Raft（自实现）  |
| 向量引擎 | HNSW            |
| RPC 框架 | gRPC + Protobuf |
| 异步 I/O | Boost.Asio      |
| 配置文件 | TOML（toml++）  |
| 日志     | spdlog          |
| 构建系统 | CMake + vcpkg   |
| 语言标准 | C++20           |

---

## 🚀 快速开始

### 环境依赖

- CMake ≥ 3.21
- C++20 编译器（GCC 11+ 或 Clang 14+）
- vcpkg，需安装以下依赖：

```bash
vcpkg install grpc boost-asio spdlog tomlplusplus
```

### 构建

```bash
git clone https://github.com/Baidou2Star/RaftVDB.git
cd RaftVDB

cmake -B build -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake
cmake --build build -j$(nproc)
```

### 启动单节点

```bash
# 编辑 config/node.toml，设置 node_id 和 peers
./build/raftvdb-node --config config/node.toml
```

### 启动 3 节点集群

为每个节点创建独立配置文件（如 `node-1.toml`、`node-2.toml`、`node-3.toml`），`peers` 列表相同，`node_id` 和端口不同：

```toml
[cluster]
node_id = "127.0.0.1:7101"
peers   = ["127.0.0.1:7101", "127.0.0.1:7102", "127.0.0.1:7103"]

[server]
grpc_port = 7101

[storage]
raft_log_dir = "./data/raft"
snapshot_dir = "./data/snapshot"
```

分别在三个终端启动：

```bash
./build/raftvdb-node --config config/node-1.toml
./build/raftvdb-node --config config/node-2.toml
./build/raftvdb-node --config config/node-3.toml
```

### 运行压测

```bash
./build/raftvdb-bench \
  --peers 127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103 \
  --threads 4 \
  --requests 10000 \
  --dim 1024 \
  --mode upsert \    # upsert | delete | search | mixed | full
  --warmup 2000 \
  --output result.csv
```

---

## ⚙️ 配置说明

`config/node.toml` 关键参数：

```toml
[raft]
heartbeat_interval_ms        = 150
election_timeout_min_ms      = 400
election_timeout_max_ms      = 700
snapshot_threshold           = 10000   # 每应用 N 条日志触发一次快照
pipeline_window_size         = 16      # 每个 peer 最大在途 Batch 数
batch_max_entries            = 64      # 单次 AppendEntries 最大日志条数
mentor_ack_timeout_ms        = 300     # Mentor 超时判定时间

[vector]
dim              = 1024
metric           = "ip"     # ip | l2sq | cos
data_type        = "f32"    # f32 | f16 | i8
initial_capacity = 100000
connectivity     = 16       # HNSW M 参数
expansion_add    = 128      # HNSW efConstruction
expansion_search = 64       # HNSW ef
```

---

## 📁 目录结构

```
├── cmd/
│   ├── node/        # 节点启动入口
│   └── bench/       # 压测工具
├── config/          # 配置文件模板
├── include/
│   ├── client/      # DBClient、DedupTable
│   ├── common/      # Config、Result<T>、Logger
│   ├── raft/        # RaftNode、WAL、Topology、Lease
│   ├── storage/     # WAL、SnapshotStore、RaftMeta
│   └── vector/      # VectorIndex、IndexMaintenance
├── proto/           # raft.proto（gRPC 服务定义）
└── src/             # 各模块实现
```

---

## 🔍 实现亮点

- **WAL**：二进制格式，每条记录带 CRC32 校验，支持段文件滚动；启动时截断末尾不完整记录实现崩溃恢复
- **快照**：写锁窗口内 `index.copy()` 克隆（极短阻塞），后台线程保存，`rename()` 原子提交，任意时刻崩溃不产生损坏快照
- **链式容错**：Leader 检测 Mentor 超时后主动探测下游 Follower，健康则提升；诊断 Mentor 扣压原因（处理能力受限 vs 链路阻塞），分别降速窗口或交换绑定处理
- **日志追赶**：先线性回退，超过阈值切指数回退；`nextIndex ≤ lastSnapshotIndex` 时自动切换为 `InstallSnapshot` 流式分块传输，避免内存峰值
- **租约读**：基于 `steady_clock`（不受 NTP 影响），`BecomeFollower` 时立即 `Invalidate()`，防止旧 Leader 租约残留

---

