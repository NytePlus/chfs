#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(new BlockManager(data_path, KDefaultBlockCnt));
  auto num_version_block = (KDefaultBlockCnt * sizeof(version_t)) / bm -> block_size();
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, num_version_block, false);
  } else {
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, num_version_block, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  usize blk_size = block_allocator_ -> bm -> block_size();
  if(block_id >= block_allocator_ -> bm -> total_blocks() || offset + len > blk_size)
    return std::vector<u8>(0);

  block_id_t v_blk = block_id * sizeof(version_t) / blk_size;
  usize v_off = block_id % (blk_size / sizeof(version_t));
  std::vector<u8> buf(blk_size);
  auto res = block_allocator_ -> bm -> read_block(v_blk, buf.data());
  if(res.is_err()) return std::vector<u8>(0);
  version_t v = reinterpret_cast<version_t *>(buf.data())[v_off];
  std::cout << block_id << "-" << v << "?=" << version << std::endl;
  if(v != version) return std::vector<u8>(0);
  auto res1 = block_allocator_ -> bm -> read_block(block_id, buf.data());
  if(res1.is_err()) return std::vector<u8>(0);
  return std::vector<u8>(buf.begin() + offset, buf.begin() + offset + len);
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  usize blk_size = block_allocator_ -> bm -> block_size();
  if(block_id >= block_allocator_ -> bm -> total_blocks() || offset + buffer.size() > blk_size)
    return false;
  auto res = block_allocator_ -> bm -> write_partial_block(block_id, buffer.data(), offset, buffer.size());
  if(res.is_err()) return false;

  block_id_t v_blk = block_id * sizeof(version_t) / blk_size;
  usize v_off = block_id % (blk_size / sizeof(version_t));
  std::vector<u8> buf(blk_size);
  auto res1 = block_allocator_ -> bm -> read_block(v_blk, buf.data());
  if(res1.is_err()) return false;
  version_t v = reinterpret_cast<version_t *>(buf.data())[v_off];
  std::cout << "write: " << block_id << "-" << v << std::endl;

  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  auto res = block_allocator_ -> allocate();

  if(res.is_err()) return std::pair<block_id_t, version_t>(0,0);
  block_id_t block_id = res.unwrap();

  usize blk_size = block_allocator_ -> bm -> block_size();
  block_id_t v_blk = block_id * sizeof(version_t) / blk_size;
  usize v_off = block_id % (blk_size / sizeof(version_t));
  std::vector<u8> buf(blk_size);
  auto res2 = block_allocator_ -> bm -> read_block(v_blk, buf.data());
  if(res2.is_err()) return std::pair<block_id_t, version_t>(0, 0);
  version_t new_v = ++ reinterpret_cast<version_t *>(buf.data())[v_off];
  auto res3 = block_allocator_ -> bm -> write_block(v_blk, buf.data());
  if(res3.is_err()) return std::pair<block_id_t, version_t>(block_id, new_v - 1);

  return std::pair<block_id_t, version_t>(block_id, new_v);
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  usize blk_size = block_allocator_ -> bm -> block_size();
  auto res = block_allocator_ -> deallocate(block_id);
  if(res.is_err())
    return false;
  block_id_t v_blk = block_id * sizeof(version_t) / blk_size;
  usize v_off = block_id % (blk_size / sizeof(version_t));
  std::vector<u8> buf(blk_size);
  auto res1 = block_allocator_ -> bm -> read_block(v_blk, buf.data());
  if(res1.is_err())
    return false;
  ++ reinterpret_cast<version_t *>(buf.data())[v_off];
  auto res2 = block_allocator_ -> bm -> write_block(v_blk, buf.data());
  if(res2.is_err())
    return false;
  return true;
}
} // namespace chfs