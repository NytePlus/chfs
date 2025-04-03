#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  if(src == "") src = filename + ":" + inode_id_to_string(id);
  else src += "/" + filename + ":" + inode_id_to_string(id);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  int pos = 0, last = 0, dot = 0;
  while(last < src.length()){
    pos = src.find('/', last);
    dot = src.find(':', last);
    if(pos == src.npos) pos = src.length();
    std::string id = src.substr(dot + 1, pos - dot - 1);
    list.push_back(DirectoryEntry{src.substr(last, dot - last), string_to_inode_id(id)});
    last = pos + 1;
  }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  int pos = 0, last = 0, dot = 0;
  while(last < src.length()){
    pos = src.find('/', last);
    dot = src.find(':', last);
    if(pos == src.npos) pos = src.length();
    if(src.substr(last, dot - last) != filename)
      res += src.substr(last, pos + 1 - last);
    last = pos + 1;
  }

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  auto inode_res = fs -> read_file(id);
  if(inode_res.is_err()){
    std::cout << "why error??\n";
    return inode_res.unwrap_error();
  }
  auto vec = inode_res.unwrap();//must asign inode_res.unwrap() to vec. Cannot use inode_res.unwrap() twice?
  std::string src(vec.begin(), vec.end());
  parse_directory(src, list);

  return KNullOk;
}

auto read_directory_from_memory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list, std::vector<std::shared_ptr<BlockOperation>> &ops) -> ChfsNullResult {
  
  // TODO: Implement this function.
  auto inode_res = fs -> read_file_from_memory(id, ops);
  if(inode_res.is_err()){
    std::cout << "why error??\n";
    return inode_res.unwrap_error();
  }
  auto vec = inode_res.unwrap();//must asign inode_res.unwrap() to vec. Cannot use inode_res.unwrap() twice?
  std::string src(vec.begin(), vec.end());
  parse_directory(src, list);

  return KNullOk;
}


// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  auto res = read_directory(this, id, list);
  if(res.is_err()){
    std::cout << "res.is_err\n";
    return res.unwrap_error();
  }
  for(DirectoryEntry dir: list){
    if(dir.name == std::string(name)){
      return ChfsResult<inode_id_t>(dir.id);
    }   
  }
  std::cout << "not exist\n";
  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

auto FileOperation::lookup_from_memory(inode_id_t id, const char *name, std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  auto res = read_directory_from_memory(this, id, list, ops);
  if(res.is_err()){
    std::cout << "res.is_err\n";
    return res.unwrap_error();
  }
  for(DirectoryEntry dir: list){
    if(dir.name == std::string(name)){
      return ChfsResult<inode_id_t>(dir.id);
    }   
  }
  std::cout << "not exist\n";
  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  std::list<DirectoryEntry> list;
  auto res = read_directory(this, id, list);
  if(res.is_err()){
    std::cout << "read dir err\n";
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  }
  std::string src = dir_list_to_string(list);

  for(DirectoryEntry dir: list)
    if(dir.name == std::string(name)){
      std::cout << "dir " << name << " already exist" << std::endl;
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  auto inode_res = alloc_inode(type);
  if(inode_res.is_err()){
    std::cout << "inode res err" << std::endl;
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  inode_id_t inode_id = inode_res.unwrap();
  src = append_to_directory(src, name, inode_id);
  auto write_res = write_file(id, std::vector<u8>(src.begin(), src.end()));
  if(write_res.is_err()){
    std::cout << "write res err" << std::endl;
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  }

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(inode_id));
}

auto FileOperation::mk_helper_with_log(inode_id_t id, const char *name, InodeType type, std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  std::list<DirectoryEntry> list;
  auto res = read_directory_from_memory(this, id, list, ops);
  if(res.is_err()){
    std::cout << "read dir err\n";
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  }
  std::string src = dir_list_to_string(list);

  for(DirectoryEntry dir: list)
    if(dir.name == std::string(name)){
      std::cout << "dir " << name << " already exist" << std::endl;
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  auto inode_res = alloc_inode_with_log(type, ops);
  if(inode_res.is_err()){
    std::cout << "inode res err" << std::endl;
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  inode_id_t inode_id = inode_res.unwrap();
  src = append_to_directory(src, name, inode_id);
  auto write_res = write_file_with_log(id, std::vector<u8>(src.begin(), src.end()), ops);
  if(write_res.is_err()){
    std::cout << "write res err" << std::endl;
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  }

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(inode_id));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  auto res = lookup(parent, name);
  if(res.is_err()){
    return res.unwrap_error();
  }
  remove_file(res.unwrap());
  std::list<DirectoryEntry> list;
  read_directory(this, parent, list);
  std::string src = rm_from_directory(dir_list_to_string(list), name).c_str();
  write_file(parent, std::vector<u8>(src.begin(), src.end()));
  
  return KNullOk;
}

auto FileOperation::unlink_with_log(inode_id_t parent, const char *name, std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  auto res = lookup_from_memory(parent, name, ops);
  if(res.is_err()){
    return res.unwrap_error();
  }
  remove_file(res.unwrap());
  std::list<DirectoryEntry> list;
  read_directory_from_memory(this, parent, list, ops);
  std::string src = rm_from_directory(dir_list_to_string(list), name).c_str();
  write_file_with_log(parent, std::vector<u8>(src.begin(), src.end()), ops);
  
  return KNullOk;
}

} // namespace chfs
