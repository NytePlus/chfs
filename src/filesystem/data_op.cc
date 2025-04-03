#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

// {Your code here}
auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t> {
  inode_id_t inode_id = static_cast<inode_id_t>(0);
  auto inode_res = ChfsResult<inode_id_t>(inode_id);

  // TODO:
  // 1. Allocate a block for the inode.
  // 2. Allocate an inode.
  // 3. Initialize the inode block
  //    and write the block back to block manager.
  auto res = this->block_allocator_ -> allocate();
  if(res.is_err())
    return ChfsResult<inode_id_t>(ErrorType::INVALID);
  block_id_t bid = res.unwrap();
  inode_res = this->inode_manager_->allocate_inode(type, bid);

  return inode_res;
}

auto FileOperation::alloc_inode_with_log(InodeType type, std::vector<std::shared_ptr<BlockOperation>> &ops) -> ChfsResult<inode_id_t> {
  inode_id_t inode_id = static_cast<inode_id_t>(0);
  auto inode_res = ChfsResult<inode_id_t>(inode_id);

  // TODO:
  // 1. Allocate a block for the inode.
  // 2. Allocate an inode.
  // 3. Initialize the inode block
  //    and write the block back to block manager.
  auto res = this->block_allocator_ -> allocate_with_log(ops);
  if(res.is_err())
    return ChfsResult<inode_id_t>(ErrorType::INVALID);
  block_id_t bid = res.unwrap();
  inode_res = this->inode_manager_->allocate_inode_with_log(type, bid, ops);

  return inode_res;
}

auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
  return this->inode_manager_->get_attr(id);
}

auto FileOperation::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  return this->inode_manager_->get_type_attr(id);
}

auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
  return this->inode_manager_->get_type(id);
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
  return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
}

auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                     u64 offset) -> ChfsResult<u64> {
  auto read_res = this->read_file(id);
  if (read_res.is_err()) {
    return ChfsResult<u64>(ErrorType::INVALID);
  }

  auto content = read_res.unwrap();
  if (offset + sz > content.size()) {
    content.resize(offset + sz);
  }
  memcpy(content.data() + offset, data, sz);

  auto write_res = this->write_file(id, content);
  if (write_res.is_err()) {
    return ChfsResult<u64>(ErrorType::INVALID);
  }
  return ChfsResult<u64>(sz);
}

// {Your code here}
auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
    -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();
  usize old_block_num = 0;
  usize new_block_num = 0;
  u64 original_file_sz = 0;

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inlined_blocks_num = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = ErrorType::INVALID;
    // I know goto is bad, but we have no choice
    goto err_ret;
  } else {
    inlined_blocks_num = inode_p->get_direct_block_num();
  }

  if (content.size() > inode_p->max_file_sz_supported()) {
    std::cerr << "file size too large: " << content.size() << " vs. "
              << inode_p->max_file_sz_supported() << std::endl;
    error_code = ErrorType::OUT_OF_RESOURCE;
    goto err_ret;
  }

  // 2. make sure whether we need to allocate more blocks
  original_file_sz = inode_p->get_size();
  old_block_num = calculate_block_sz(original_file_sz, block_size);
  new_block_num = calculate_block_sz(content.size(), block_size);

  if (new_block_num > old_block_num) {
    // If we need to allocate more blocks.
    for (usize idx = old_block_num; idx < new_block_num; ++idx) {

      // TODO: Implement the case of allocating more blocks.
      // 1. Allocate a block.
      // 2. Fill the allocated block id to the inode.
      //    You should pay attention to the case of indirect block.
      //    You may use function `get_or_insert_indirect_block`
      //    in the case of indirect block.
      if(inode_p -> is_direct_block(idx)){
        auto res = block_allocator_->allocate();
        if(res.is_err()){
          error_code = ErrorType::OUT_OF_RESOURCE;
          goto err_ret; 
        }
        inode_p -> set_block_direct(idx, res.unwrap());
      }
      else{
        block_manager_->read_block(inode_p -> get_or_insert_indirect_block(block_allocator_).unwrap(), indirect_block.data());
        auto res = block_allocator_->allocate();
        if(res.is_err()){
          error_code = ErrorType::OUT_OF_RESOURCE;
          goto err_ret; 
        }
        *reinterpret_cast<block_id_t*>(&indirect_block[(idx - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]) = res.unwrap();
        inode_p -> write_indirect_block(block_manager_, indirect_block);
      }
    }

  } else {
    // We need to free the extra blocks.
    for (usize idx = new_block_num; idx < old_block_num; ++idx) {
      if (inode_p->is_direct_block(idx)) {

        // TODO: Free the direct extra block.
        auto res = block_allocator_ -> deallocate(inode_p -> blocks[idx]);
        if(res.is_err()){
          error_code = ErrorType::OUT_OF_RESOURCE;
          goto err_ret; 
        }
      } else {
        // TODO: Free the indirect extra block.
        block_manager_->read_block(inode_p -> get_indirect_block_id(), indirect_block.data());
        auto res = block_allocator_ -> deallocate(*reinterpret_cast<block_id_t*>(&indirect_block[(idx - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]));
        if(res.is_err()){
          error_code = ErrorType::OUT_OF_RESOURCE;
          goto err_ret; 
        }
      }
    }

    // If there are no more indirect blocks.
    if (old_block_num > inlined_blocks_num &&
        new_block_num <= inlined_blocks_num && true) {
      auto res =
          this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
      if (res.is_err()) {
        error_code = ErrorType::INVALID;
        goto err_ret;
      }
      indirect_block.clear();
      inode_p->invalid_indirect_block_id();
    }
  }

  // 3. write the contents
  inode_p->inner_attr.size = content.size();
  inode_p->inner_attr.mtime = time(0);

  {
    auto block_idx = 0;
    u64 write_sz = 0;
    block_id_t bid;

    while (write_sz < content.size()) {
      auto sz = ((content.size() - write_sz) > block_size)
                    ? block_size
                    : (content.size() - write_sz);
      std::vector<u8> buffer(block_size);
      memcpy(buffer.data(), content.data() + write_sz, sz);

      if (inode_p->is_direct_block(block_idx)) {

        // TODO: Implement getting block id of current direct block.
        bid = inode_p -> blocks[block_idx];

      } else {
        // TODO: Implement getting block id of current indirect block.
        block_manager_->read_block(inode_p -> get_indirect_block_id(), indirect_block.data());
        bid = *reinterpret_cast<block_id_t*>(&indirect_block[(block_idx - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]);
      }

      // TODO: Write to current block.
      block_manager_->write_block(bid, buffer.data());

      write_sz += sz;
      block_idx += 1;
    }
  }

  // finally, update the inode
  {
    inode_p->inner_attr.set_all_time(time(0));

    auto write_res =
        this->block_manager_->write_block(inode_res.unwrap(), inode.data());
    if (write_res.is_err()) {
      error_code = ErrorType::INVALID;
      goto err_ret;
    }
    if (indirect_block.size() != 0) {
      write_res =
          inode_p->write_indirect_block(this->block_manager_, indirect_block);
      if (write_res.is_err()) {
        error_code = ErrorType::INVALID;
        goto err_ret;
      }
    }
  }

  return KNullOk;

err_ret:
  // std::cerr << "write file return error: " << (int)error_code << std::endl;
  return ChfsNullResult(error_code);
}

auto FileOperation::write_file_with_log(inode_id_t id, const std::vector<u8> &content, std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();
  usize old_block_num = 0;
  usize new_block_num = 0;
  u64 original_file_sz = 0;

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inlined_blocks_num = 0;

  auto inode_res = this->inode_manager_->read_inode_from_memory(id, inode, ops);
  if (inode_res.is_err()) {
    error_code = ErrorType::INVALID;
    // I know goto is bad, but we have no choice
    goto err_ret;
  } else {
    inlined_blocks_num = inode_p->get_direct_block_num();
  }

  if (content.size() > inode_p->max_file_sz_supported()) {
    std::cerr << "file size too large: " << content.size() << " vs. "
              << inode_p->max_file_sz_supported() << std::endl;
    error_code = ErrorType::OUT_OF_RESOURCE;
    goto err_ret;
  }

  // 2. make sure whether we need to allocate more blocks
  original_file_sz = inode_p->get_size();
  old_block_num = calculate_block_sz(original_file_sz, block_size);
  new_block_num = calculate_block_sz(content.size(), block_size);

  if (new_block_num > old_block_num) {
    // If we need to allocate more blocks.
    for (usize idx = old_block_num; idx < new_block_num; ++idx) {

      // TODO: Implement the case of allocating more blocks.
      // 1. Allocate a block.
      // 2. Fill the allocated block id to the inode.
      //    You should pay attention to the case of indirect block.
      //    You may use function `get_or_insert_indirect_block`
      //    in the case of indirect block.
      auto res = block_allocator_->allocate_with_log(ops);
      if(res.is_err()){
        std::cout << "alloc err\n";
        error_code = ErrorType::BadResponse;
        goto err_ret;
      }
      if(inode_p -> is_direct_block(idx)){
        std::cout << "write with log\n";
        inode_p -> set_block_direct(idx, res.unwrap());
      }else{
        block_manager_->read_block_from_memory(inode_p -> get_or_insert_indirect_block(block_allocator_).unwrap(), indirect_block.data(), ops);
        *reinterpret_cast<block_id_t*>(&indirect_block[(idx - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]) = res.unwrap();
        auto res1 = inode_p -> write_indirect_block_with_log(block_manager_, indirect_block, ops);
        if(res1.is_err()){
          error_code = ErrorType::BadResponse;
          goto err_ret;
        }
      }
    }

  } else {
    // We need to free the extra blocks.
    for (usize idx = new_block_num; idx < old_block_num; ++idx) {
      if (inode_p->is_direct_block(idx)) {

        // TODO: Free the direct extra block.
        auto res = block_allocator_ -> deallocate_with_log(inode_p -> blocks[idx], ops);
        if(res.is_err()){
          std::cout << "dealloc err\n";
          error_code = ErrorType::OUT_OF_RESOURCE;
          goto err_ret; 
        }
      } else {
        // TODO: Free the indirect extra block.
        block_manager_->read_block_from_memory(inode_p -> get_indirect_block_id(), indirect_block.data(), ops);
        auto res = block_allocator_ -> deallocate_with_log(*reinterpret_cast<block_id_t*>(&indirect_block[(idx - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]), ops);
        if(res.is_err()){
          std::cout << "dealloc err\n";
          error_code = ErrorType::OUT_OF_RESOURCE;
          goto err_ret; 
        }
      }
    }

    // If there are no more indirect blocks.
    if (old_block_num > inlined_blocks_num &&
        new_block_num <= inlined_blocks_num && true) {
      auto res =
          this->block_allocator_->deallocate_with_log(inode_p->get_indirect_block_id(), ops);
      if (res.is_err()) {
          std::cout << "dealloc err\n";
        error_code = ErrorType::INVALID;
        goto err_ret;
      }
      indirect_block.clear();
      inode_p->invalid_indirect_block_id();
    }
  }

  // 3. write the contents
  inode_p->inner_attr.size = content.size();
  inode_p->inner_attr.mtime = time(0);

  {
    auto block_idx = 0;
    u64 write_sz = 0;
    block_id_t bid;

    while (write_sz < content.size()) {
      auto sz = ((content.size() - write_sz) > block_size)
                    ? block_size
                    : (content.size() - write_sz);
      std::vector<u8> buffer(block_size);
      memcpy(buffer.data(), content.data() + write_sz, sz);

      if (inode_p->is_direct_block(block_idx)) {

        // TODO: Implement getting block id of current direct block.
        bid = inode_p -> blocks[block_idx];

      } else {
        // TODO: Implement getting block id of current indirect block.
        block_manager_->read_block_from_memory(inode_p -> get_indirect_block_id(), indirect_block.data(), ops);
        bid = *reinterpret_cast<block_id_t*>(&indirect_block[(block_idx - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]);
      }

      // TODO: Write to current block.
      std::cout << "write with log\n";
      auto res = block_manager_->write_block_to_memory(bid, buffer.data(), ops);
      if(res.is_err()){
          error_code = ErrorType::BadResponse;
          goto err_ret;
        }

      write_sz += sz;
      block_idx += 1;
    }
  }

  // finally, update the inode
  {
    inode_p->inner_attr.set_all_time(time(0));

    auto write_res =
        this->block_manager_->write_block_to_memory(inode_res.unwrap(), inode.data(), ops);
    if (write_res.is_err()) {
      error_code = ErrorType::INVALID;
      goto err_ret;
    }
    if (indirect_block.size() != 0) {
      write_res =
          inode_p->write_indirect_block_with_log(this->block_manager_, indirect_block, ops);
      if (write_res.is_err()) {
        error_code = ErrorType::INVALID;
        goto err_ret;
      }
    }
  }

  return KNullOk;

err_ret:
  std::cerr << "write file return error: " << (int)error_code << std::endl;
  return ChfsNullResult(error_code);
}

auto FileOperation::read_file_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &ops) -> ChfsResult<std::vector<u8>> {
  auto error_code = ErrorType::DONE;
  std::vector<u8> content;

  const auto block_size = this->block_manager_->block_size();

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  u64 file_sz = 0;
  u64 read_sz = 0;

  auto inode_res = this->inode_manager_->read_inode_from_memory(id, inode, ops);
  if (inode_res.is_err()) {
    error_code = ErrorType::INVALID;
    // I know goto is bad, but we have no choice
    goto err_ret;
  }

  file_sz = inode_p->get_size();
  content.reserve(file_sz);

  // Now read the file
  while (read_sz < file_sz) {
    auto sz = ((inode_p->get_size() - read_sz) > block_size)
                  ? block_size
                  : (inode_p->get_size() - read_sz);

    // Get current block id.
    block_id_t bid;
    if (inode_p->is_direct_block(read_sz / block_size)) {
      // TODO: Implement the case of direct block.
      bid = inode_p->blocks[read_sz / block_size];
      
    } else {
      // TODO: Implement the case of indirect block.
      block_manager_->read_block_from_memory(inode_p -> get_indirect_block_id(), indirect_block.data(), ops);

      bid = *reinterpret_cast<block_id_t*>(&indirect_block[(read_sz / block_size - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]);
    }

    // TODO: Read from current block and store to `content`.
    std::vector<u8> buffer(block_size);
    block_manager_->read_block_from_memory(bid, buffer.data(), ops);
    content.insert(content.end(), buffer.begin(), buffer.begin() + sz);
    read_sz += sz;
  }
  // std:: cout << "read_sz: " << read_sz << "\ncontent.size: " << content.size() << std::endl; 
  return ChfsResult<std::vector<u8>>(std::move(content));

err_ret:
  return ChfsResult<std::vector<u8>>(error_code);
}

// {Your code here}
auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
  auto error_code = ErrorType::DONE;
  std::vector<u8> content;

  const auto block_size = this->block_manager_->block_size();

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  u64 file_sz = 0;
  u64 read_sz = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = ErrorType::INVALID;
    // I know goto is bad, but we have no choice
    goto err_ret;
  }

  file_sz = inode_p->get_size();
  content.reserve(file_sz);

  // Now read the file
  while (read_sz < file_sz) {
    auto sz = ((inode_p->get_size() - read_sz) > block_size)
                  ? block_size
                  : (inode_p->get_size() - read_sz);

    // Get current block id.
    block_id_t bid;
    if (inode_p->is_direct_block(read_sz / block_size)) {
      // TODO: Implement the case of direct block.
      bid = inode_p->blocks[read_sz / block_size];
      
    } else {
      // TODO: Implement the case of indirect block.
      block_manager_->read_block(inode_p -> get_indirect_block_id(), indirect_block.data());

      bid = *reinterpret_cast<block_id_t*>(&indirect_block[(read_sz / block_size - inode_p -> get_direct_block_num()) * sizeof(block_id_t) / sizeof(u8)]);
    }

    // TODO: Read from current block and store to `content`.
    std::vector<u8> buffer(block_size);
    block_manager_->read_block(bid, buffer.data());
    content.insert(content.end(), buffer.begin(), buffer.begin() + sz);
    read_sz += sz;
  }
  // std:: cout << "read_sz: " << read_sz << "\ncontent.size: " << content.size() << std::endl; 
  return ChfsResult<std::vector<u8>>(std::move(content));

err_ret:
  return ChfsResult<std::vector<u8>>(error_code);
}

auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
    -> ChfsResult<std::vector<u8>> {
  auto res = read_file(id);
  if (res.is_err()) {
    return res;
  }

  auto content = res.unwrap();
  return ChfsResult<std::vector<u8>>(
      std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
}

auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
  auto attr_res = this->getattr(id);
  if (attr_res.is_err()) {
    return ChfsResult<FileAttr>(ErrorType::INVALID);
  }

  auto attr = attr_res.unwrap();
  auto file_content = this->read_file(id);
  if (file_content.is_err()) {
    return ChfsResult<FileAttr>(ErrorType::INVALID);
  }

  auto content = file_content.unwrap();

  if (content.size() != sz) {
    content.resize(sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err()) {
      return ChfsResult<FileAttr>(ErrorType::INVALID);
    }
  }

  attr.size = sz;
  return ChfsResult<FileAttr>(attr);
}

} // namespace chfs
