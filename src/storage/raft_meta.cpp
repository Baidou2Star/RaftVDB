#include "storage/raft_meta.hpp"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <vector>

#include "raft.pb.h"

namespace {

namespace proto = raftvdb::proto;

constexpr std::string_view kTempSuffix = ".tmp";

Result<void> WriteAll(int fd, const char* data, size_t size) {
    size_t written = 0;
    while (written < size) {
        const ssize_t rc = ::write(fd, data + written, size - written);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Result<void>::Err("写入 meta.tmp 失败: " + std::string(std::strerror(errno)));
        }
        written += static_cast<size_t>(rc);
    }
    return Result<void>::Ok();
}

Result<void> FsyncDirectory(const std::filesystem::path& dir) {
    const int dir_fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) {
        return Result<void>::Err("打开元数据目录失败: " + std::string(std::strerror(errno)));
    }

    const int sync_rc = ::fsync(dir_fd);
    const int close_rc = ::close(dir_fd);
    if (sync_rc != 0) {
        return Result<void>::Err("fsync 元数据目录失败: " + std::string(std::strerror(errno)));
    }
    if (close_rc != 0) {
        return Result<void>::Err("关闭元数据目录失败: " + std::string(std::strerror(errno)));
    }
    return Result<void>::Ok();
}

} // namespace

Result<void> RaftMeta::Save(const std::string& path) const {
    try {
        const std::filesystem::path final_path(path);
        const std::filesystem::path parent_dir =
            final_path.has_parent_path() ? final_path.parent_path() : std::filesystem::current_path();
        std::filesystem::create_directories(parent_dir);

        proto::RaftMetaRecord record;
        record.set_current_term(current_term);
        record.set_voted_for(voted_for);

        std::string bytes;
        if (!record.SerializeToString(&bytes)) {
            return Result<void>::Err("protobuf 序列化 RaftMeta 失败");
        }

        const std::filesystem::path tmp_path = final_path.string() + std::string(kTempSuffix);
        const int fd = ::open(tmp_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0) {
            return Result<void>::Err("创建 meta.tmp 失败: " + std::string(std::strerror(errno)));
        }

        auto write_result = WriteAll(fd, bytes.data(), bytes.size());
        if (!write_result) {
            ::close(fd);
            std::error_code ec;
            std::filesystem::remove(tmp_path, ec);
            return write_result;
        }

        if (::fdatasync(fd) != 0) {
            const auto message = "fdatasync meta.tmp 失败: " + std::string(std::strerror(errno));
            ::close(fd);
            std::error_code ec;
            std::filesystem::remove(tmp_path, ec);
            return Result<void>::Err(message);
        }

        if (::close(fd) != 0) {
            std::error_code ec;
            std::filesystem::remove(tmp_path, ec);
            return Result<void>::Err("关闭 meta.tmp 失败: " + std::string(std::strerror(errno)));
        }

        if (::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
            const auto message = "rename meta.tmp 失败: " + std::string(std::strerror(errno));
            std::error_code ec;
            std::filesystem::remove(tmp_path, ec);
            return Result<void>::Err(message);
        }

        auto fsync_dir_result = FsyncDirectory(parent_dir);
        if (!fsync_dir_result) {
            return fsync_dir_result;
        }

        return Result<void>::Ok();
    } catch (const std::exception& error) {
        return Result<void>::Err("保存 RaftMeta 失败: " + std::string(error.what()));
    }
}

Result<RaftMeta> RaftMeta::Load(const std::string& path) {
    try {
        const std::filesystem::path final_path(path);
        if (!std::filesystem::exists(final_path)) {
            // Missing meta.bin means this node has not persisted term/vote yet.
            return Result<RaftMeta>::Ok(RaftMeta{});
        }

        const auto file_size = std::filesystem::file_size(final_path);
        std::vector<char> bytes(file_size);

        const int fd = ::open(final_path.c_str(), O_RDONLY);
        if (fd < 0) {
            return Result<RaftMeta>::Err("打开 meta.bin 失败: " + std::string(std::strerror(errno)));
        }

        size_t read_offset = 0;
        while (read_offset < bytes.size()) {
            const ssize_t rc =
                ::read(fd, bytes.data() + read_offset, bytes.size() - read_offset);
            if (rc < 0) {
                if (errno == EINTR) {
                    continue;
                }
                const auto message = "读取 meta.bin 失败: " + std::string(std::strerror(errno));
                ::close(fd);
                return Result<RaftMeta>::Err(message);
            }
            if (rc == 0) {
                break;
            }
            read_offset += static_cast<size_t>(rc);
        }

        if (::close(fd) != 0) {
            return Result<RaftMeta>::Err("关闭 meta.bin 失败: " + std::string(std::strerror(errno)));
        }
        if (read_offset != bytes.size()) {
            return Result<RaftMeta>::Err("meta.bin 内容不完整");
        }

        proto::RaftMetaRecord record;
        if (!record.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
            return Result<RaftMeta>::Err("protobuf 反序列化 RaftMeta 失败");
        }

        RaftMeta meta;
        meta.current_term = record.current_term();
        meta.voted_for = record.voted_for();
        return Result<RaftMeta>::Ok(std::move(meta));
    } catch (const std::exception& error) {
        return Result<RaftMeta>::Err("加载 RaftMeta 失败: " + std::string(error.what()));
    }
}
