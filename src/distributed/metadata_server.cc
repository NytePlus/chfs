#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  global_mtx.lock();
  if(is_log_enabled_){
    std::vector<std::shared_ptr<BlockOperation>> ops;
    inode_id_t inode_id = 0;
    if(type == DirectoryType){
      auto res = operation_ -> mkdir_with_log(parent, name.data(), ops);
      std::cout << "[";
      for(auto op : ops){
        std::cout << "id: " << op -> block_id_ << " data: " << op -> new_block_state_.data() << std::endl;
      }
      std::cout <<"]";
      commit_log -> append_log(commit_log -> get_id(), ops);
      if(res.is_err()) std::cout << "err\n";
      inode_id = res.is_err() ? 0 : res.unwrap();
    }else if(type == RegularFileType){
      auto res = operation_ -> mkfile_with_log(parent, name.data(), ops);
      commit_log -> append_log(commit_log -> get_id(), ops);
      inode_id = res.is_err() ? 0 : res.unwrap();
    }

    for(auto &op : ops){
        auto write_res = operation_ -> block_manager_ -> write_block(op -> block_id_, op -> new_block_state_.data());
        if(write_res.is_err()){
          global_mtx.unlock();
          return KInvalidInodeID;
        }
      }
    global_mtx.unlock();
    return inode_id;

  }else{
    if(type == DirectoryType){
      auto res = operation_ -> mkdir(parent, name.data());
      global_mtx.unlock();
      return res.is_err() ? 0 : res.unwrap();
    }else if(type == RegularFileType){
      auto res = operation_ -> mkfile(parent, name.data());
      global_mtx.unlock();
      return res.is_err() ? 0 : res.unwrap();
    }else{
      global_mtx.unlock();
      return 0;
    }
  }
  
  global_mtx.unlock();
  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  global_mtx.lock();

  if(is_log_enabled_){
    std::vector<std::shared_ptr<BlockOperation>> ops;

    auto res = operation_ -> lookup_from_memory(parent, name.data(), ops); // no need to from memory
    if(res.is_err()){
      global_mtx.unlock();
      return false;
    }
    auto inode_id = res.unwrap();
    auto res2 = operation_ -> gettype(inode_id); // no need to from memory
    if(res2.is_err()){
      global_mtx.unlock();
      return false;
    }
    auto type = res2.unwrap();

    if(type == InodeType::FILE){
      std::vector<BlockInfo> blks = get_block_map(inode_id); // no need to from memory
      auto res = operation_ -> inode_manager_ -> get(inode_id); // no need to from memory
      if(res.is_err()){
        global_mtx.unlock();
        return false;
      }
      operation_ -> inode_manager_ -> free_inode_with_log(inode_id, ops);
      operation_ -> block_allocator_ -> deallocate_with_log(res.unwrap(), ops);
      for(int i = 0; i < blks.size(); i ++){
        std::shared_ptr<RpcClient> client = clients_[std::get<1>(blks[i])];
        auto res = client -> call("free block", std::get<0>(blks[i]));
        if(res.is_err() || !res.unwrap()){
          global_mtx.unlock();
          return false;
        }
      }
      std::list<DirectoryEntry> list;
      read_directory_from_memory(operation_.get(), parent, list, ops);
      std::string src = rm_from_directory(dir_list_to_string(list), name).c_str();
      operation_ -> write_file_with_log(parent, std::vector<u8>(src.begin(), src.end()), ops);

      commit_log -> append_log(commit_log -> get_id(), ops);
    }else if(type == InodeType::Directory){
      auto res = operation_ -> unlink_with_log(parent, name.data(), ops);
      if(res.is_err()){
        global_mtx.unlock();
        return false;
      }
      commit_log -> append_log(commit_log -> get_id(), ops);
    }else{
      global_mtx.unlock();
      return false;
    }

    for(auto &op : ops){
        auto write_res = operation_ -> block_manager_ -> write_block(op -> block_id_, op -> new_block_state_.data());
        if(write_res.is_err()){
          global_mtx.unlock();
          return KInvalidInodeID;
        }
      }
    global_mtx.unlock();
    return true;

  }else{

    auto res = operation_ -> lookup(parent, name.data());
    if(res.is_err()){
      global_mtx.unlock();
      return false;
    }
    auto inode_id = res.unwrap();
    auto res2 = operation_ -> gettype(inode_id);
    if(res2.is_err()){
      global_mtx.unlock();
      return false;
    }
    auto type = res2.unwrap();

    if(type == InodeType::FILE){
      std::vector<BlockInfo> blks = get_block_map(inode_id);
      auto res = operation_ -> inode_manager_ -> get(inode_id);
      if(res.is_err()){
        global_mtx.unlock();
        return false;
      }
      operation_ -> inode_manager_ -> free_inode(inode_id);
      operation_ -> block_allocator_ -> deallocate(res.unwrap());
      for(int i = 0; i < blks.size(); i ++){
        std::shared_ptr<RpcClient> client = clients_[std::get<1>(blks[i])];
        auto res = client -> call("free block", std::get<0>(blks[i]));
        if(res.is_err() || !res.unwrap()){
          global_mtx.unlock();
          return false;
        }
      }
      std::list<DirectoryEntry> list;
      read_directory(operation_.get(), parent, list);
      std::string src = rm_from_directory(dir_list_to_string(list), name).c_str();
      operation_ -> write_file(parent, std::vector<u8>(src.begin(), src.end()));
      global_mtx.unlock();
      return true;
    }else if(type == InodeType::Directory){
      auto res = operation_ -> unlink(parent, name.data());
      if(res.is_err()){
        global_mtx.unlock();
        return false;
      }
      global_mtx.unlock();
      return true;
    }
  }
  
  global_mtx.unlock();
  return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  auto res = operation_->lookup(parent, name.data());
  if(res.is_err()){
    return KInvalidInodeID;
  }
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  if(id > operation_ -> inode_manager_ -> get_max_inode_supported())
    return {};
  auto res = operation_ -> gettype(id);
  if(res.is_err())
    return {};
  if(res.unwrap() == InodeType::FILE){
    std::vector<BlockInfo> block_map;
    auto res = operation_ -> inode_manager_ -> get(id);
    if(res.is_err() || res.unwrap() == KInvalidBlockID) 
      return {};
    usize blk_size = operation_ -> block_manager_ -> block_size();
    std::vector<u8> buffer(blk_size);
    operation_ -> block_manager_ -> read_block(res.unwrap(), buffer.data());
    Inode *inode = reinterpret_cast<Inode *>(buffer.data());
    
    for(int i = 0; i < inode -> get_nblocks() && inode -> blocks[i] != KInvalidBlockID; i += 2){
      block_id_t blk_id = inode -> blocks[i];
      mac_id_t mac_id = static_cast<mac_id_t>(inode -> blocks[i + 1]);

      std::map<mac_id_t, std::shared_ptr<RpcClient>>::iterator it = clients_.find(mac_id);
      if(it == clients_.end())
        return {};
      block_id_t v_blk = blk_id * sizeof(version_t) / blk_size;
      usize v_off = blk_id % (blk_size / sizeof(version_t));
  
      auto res = it -> second -> call("read_data", v_blk, v_off * sizeof(version_t), sizeof(version_t), 0);
      std::vector<u8> data = res.unwrap() -> as<std::vector<u8>>();
      if(data.size() == 0)
        return {};
      block_map.push_back(BlockInfo(blk_id, mac_id, *reinterpret_cast<version_t *>(data.data())));
    }
    return block_map;
  }

  return {};
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  global_mtx.lock();
  if(id > operation_ -> inode_manager_ -> get_max_inode_supported()){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  usize blk_size = operation_ -> block_manager_ -> block_size();
  std::vector<u8> buf(blk_size);
  auto inode = reinterpret_cast<Inode *>(buf.data());
  auto res = operation_ -> inode_manager_ -> get(id);
  if(res.is_err()){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  auto inode_block_id = res.unwrap();
  if(inode_block_id == KInvalidBlockID){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  auto res1 = operation_->block_manager_->read_block(inode_block_id, buf.data());
  if(res1.is_err()){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  usize blk_num = inode -> get_size() / blk_size;
  if((blk_num + 1) * 2 > inode -> get_nblocks()){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  mac_id_t mac_id = generator.rand(1, num_data_servers);
  auto it = clients_.find(mac_id);
  if(it == clients_.end()){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  auto res2 = it -> second ->call("alloc_block");
  if(res2.is_err()){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, mac_id, 0);
  }
  std::pair<chfs::block_id_t, chfs::version_t> pair = res2.unwrap()->as<std::pair<chfs::block_id_t, chfs::version_t>>();

  inode -> set_size(inode -> get_size() + blk_size);
  inode -> set_block_direct(blk_num * 2, pair.first);
  inode -> set_block_direct(blk_num * 2 + 1, mac_id);

  auto write_res = operation_->block_manager_->write_block(inode_block_id, buf.data());
  if(write_res.is_err()){
    global_mtx.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  global_mtx.unlock();
  return BlockInfo(pair.first, mac_id, pair.second);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  global_mtx.lock();
  if(id > operation_ -> inode_manager_ -> get_max_inode_supported() || block_id == KInvalidBlockID){
    global_mtx.unlock();
    return false;
  }
  usize blk_size = operation_ -> block_manager_ -> block_size();
  std::vector<u8> buf(blk_size);
  auto res = operation_ -> inode_manager_ -> get(id);
  if(res.is_err() || res.unwrap() == KInvalidBlockID){
    global_mtx.unlock();
    return false;
  }

  block_id_t inode_blk_id = res.unwrap();
  operation_ -> block_manager_ -> read_block(inode_blk_id, buf.data());
  Inode *inode = reinterpret_cast<Inode*>(buf.data());
  int off = -1;
  for(int i = 0; i < inode -> get_nblocks(); i += 2)
    if(inode -> blocks[i] == block_id && inode -> blocks[i + 1] == machine_id){
      off = i;
      break;
    }
  if(!~off){
    global_mtx.unlock();
    return false;
  }
  auto res1 = clients_[machine_id] -> call("free_block", block_id);
  if(res1.is_err() || !res1.unwrap()){
    global_mtx.unlock();
    std::cout << "free err\n";
    return false;
  }
  for(int i = off; i < inode -> get_nblocks() - 2; i += 2){
    inode -> blocks[i] = inode -> blocks[i + 2];
    inode -> blocks[i + 1] = inode -> blocks[i + 3];
  }
  inode -> blocks[inode -> get_nblocks() - 2] = KInvalidInodeID;
  inode -> blocks[inode -> get_nblocks() - 1] = 0;
  inode -> set_size(inode -> get_size() - blk_size);
  auto res2 = operation_ -> block_manager_ -> write_block(inode_blk_id, buf.data());
  if(res2.is_err()){
    global_mtx.unlock();
    std::cout << "write err\n";
    return false;
  }
  global_mtx.unlock();

  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  std::list<DirectoryEntry> list;
  auto res = read_directory(operation_.get(), node, list);
  if(res.is_err()){
    return {};
  }
  std::vector<std::pair<std::string, inode_id_t>> res1;
  for(std::list<DirectoryEntry>::iterator it = list.begin(); it != list.end(); it ++)
    res1.push_back(std::make_pair(it -> name, it -> id));

  return res1;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  global_mtx.lock();
  auto res = operation_ -> get_type_attr(id);
  if(res.is_err()){
    global_mtx.unlock();
    return {};
  }
  InodeType inode_type = res.unwrap().first;
  FileAttr file_attr = res.unwrap().second;
  
  global_mtx.unlock();
  return std::tuple<u64, u64, u64, u64, u8>(file_attr.size, file_attr.atime, file_attr.mtime, file_attr.ctime, static_cast<u8>(inode_type));
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs