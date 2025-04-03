#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto res = metadata_server_ -> call("mknode", static_cast<u8>(type), parent, name);
  if(res.is_err()){
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  inode_id_t inode_id = res.unwrap() -> as<inode_id_t>();
  if(inode_id == KInvalidInodeID)
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto res = metadata_server_ -> call("unlink", parent, name);
  if(res.is_err())
    return ChfsNullResult(res.unwrap_error());
  if(!res.unwrap() -> as<bool>())
    return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto res = metadata_server_ -> call("lookup", parent, name);
  if(res.is_err())
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  inode_id_t inode_id = res.unwrap() -> as<inode_id_t>();
  if(inode_id == KInvalidInodeID)
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  auto res = metadata_server_ -> call("readdir", id);
  if(res.is_err())
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(res.unwrap_error());
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>());
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  auto res = metadata_server_ -> call("get_type_attr", id);
  if(res.is_err())
    return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
  std::tuple<u64, u64, u64, u64, u8> type_attr = res.unwrap() -> as<std::tuple<u64, u64, u64, u64, u8>>();
  
  u8 type = std::get<4>(type_attr);
  InodeType inode_type;
  if(type == DirectoryType)
    inode_type = InodeType::Directory;
  else if(type == RegularFileType)
    inode_type = InodeType::FILE;
  else
    inode_type = InodeType::Unknown;

  FileAttr file_attr;
  file_attr.size = std::get<0>(type_attr);
  file_attr.atime = std::get<1>(type_attr);
  file_attr.mtime = std::get<2>(type_attr);
  file_attr.ctime = std::get<3>(type_attr);
  return ChfsResult<std::pair<InodeType, FileAttr>>(std::make_pair(inode_type, file_attr));
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  const usize blk_size = DiskBlockSize;
  auto blk_map_res = metadata_server_ -> call("get_block_map", id);
  if(blk_map_res.is_err())
    return ChfsResult<std::vector<u8>>(blk_map_res.unwrap_error());
  std::vector<chfs::BlockInfo> blk_infos = blk_map_res.unwrap() -> as<std::vector<chfs::BlockInfo>>();
  usize file_size = blk_infos.size() * blk_size;
  if(offset + size > file_size)
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
  
  std::vector<u8> buf(size);

  usize start_idx = offset / blk_size, start_off = offset % blk_size;
  usize end_idx = ((offset + size) % blk_size) ? ((offset + size) / blk_size + 1) : ((offset + size) / blk_size), end_off = ((offset + size) % blk_size) ? ((offset + size) % blk_size) : blk_size;
  usize off = 0;

  for(int i = start_idx; i < end_idx; i ++){
    block_id_t blk_id = std::get<0>(blk_infos[i]);
    mac_id_t mac_id = std::get<1>(blk_infos[i]);
    version_t version = std::get<2>(blk_infos[i]);
    std::cout << "[" << blk_id << ", " << mac_id << ", " << version << "]\n";

    auto mac_it = data_servers_.find(mac_id);
    if(mac_it == data_servers_.end())
      return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
    auto read_res = mac_it -> second -> call("read_data", blk_id, 0, blk_size, version);
    if(read_res.is_err())
      return ChfsResult<std::vector<u8>>(read_res.unwrap_error());

    std::vector<u8> data = read_res.unwrap()->as<std::vector<u8>>();
    if(i == start_idx && i == end_idx - 1)
      return ChfsResult<std::vector<u8>>(std::vector<u8>(data.begin() + start_off, data.begin() + end_off));
    else if(i == start_idx){
      memcpy(buf.data() + off, data.data() + start_off, blk_size - start_off);
      off += blk_size - start_off;
    }else if(i == end_idx - 1){
      memcpy(buf.data() + off, data.data(), end_off);
      off += end_off;
    }else{
      memcpy(buf.data() + off, data.data(), blk_size);
      off += blk_size;
    }
  }
  return ChfsResult<std::vector<u8>>(buf);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  const usize blk_size = DiskBlockSize;
  usize length = data.size();
  auto blk_map_res = metadata_server_ -> call("get_block_map", id);
  if(blk_map_res.is_err())
    return ChfsNullResult(blk_map_res.unwrap_error());
  
  auto blk_infos = blk_map_res.unwrap() -> as<std::vector<chfs::BlockInfo>>();
  auto file_size = blk_infos.size() * blk_size;

  if(offset + length > file_size){
    usize new_blk_num = ((offset + length) % blk_size) ? ((offset + length) / blk_size + 1) : ((offset + length) / blk_size);
    usize old_blk_num = blk_infos.size();
    
    for(auto i = old_blk_num; i < new_blk_num; i ++){
      auto alloc_res = metadata_server_ -> call("alloc_block", id);
      if(alloc_res.is_err())
        return ChfsNullResult(alloc_res.unwrap_error());

      blk_infos.push_back(alloc_res.unwrap() -> as<BlockInfo>());
    }
  }

  usize start_idx = offset / blk_size, start_off = offset % blk_size;
  usize end_idx = ((offset + length) % blk_size) ? ((offset + length) / blk_size + 1) : ((offset + length) / blk_size), end_off = ((offset + length) % blk_size) ? ((offset + length) % blk_size) : blk_size;
  usize off = 0;
  for(int i = start_idx; i < end_idx; i ++){
    block_id_t blk_id = std::get<0>(blk_infos[i]);
    mac_id_t mac_id = std::get<1>(blk_infos[i]);

    std::vector<u8> buf;
    usize write_off = 0;
    if(i == start_idx && i == end_idx - 1){
      buf = data;
      write_off = start_off;
      off += start_off;
    }if(i == start_idx){
      buf.resize(blk_size - start_off);
      memcpy(buf.data(), data.data() + off, blk_size - start_off);
      write_off = start_off;
      off += blk_size - start_off;
    }else if(i == end_idx - 1){
      buf.resize(end_off);
      memcpy(buf.data(), data.data() + off, end_off);
      write_off = 0;
      off += end_off;
    }else{
      buf.resize(blk_size);
      memcpy(buf.data(), data.data() + off, blk_size);
      write_off = 0;
      off += blk_size;
    }

    auto mac_it = data_servers_.find(mac_id);
    if(mac_it == data_servers_.end())
      return ChfsNullResult(ErrorType::INVALID_ARG);
    auto write_res = mac_it -> second -> call("write_data", blk_id, write_off, buf);
    if(write_res.is_err())
      return ChfsNullResult(write_res.unwrap_error());

    if(!write_res.unwrap() -> as<bool>())
      return ChfsNullResult(ErrorType::INVALID);
  }   
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  auto res = metadata_server_ -> call("free_block", id, block_id, mac_id);
  if(res.is_err())
    return ChfsNullResult(res.unwrap_error());
  if(!res.unwrap() -> as<bool>())
    return ChfsNullResult(ErrorType::NotExist);
  return KNullOk;
}

} // namespace chfs