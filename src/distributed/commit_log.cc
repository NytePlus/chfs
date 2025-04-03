#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm), log_entry_num(0), log_offset(0), txn_id(1) {
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  return log_entry_num;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  log_mtx.lock();

  ++ log_entry_num;
  block_id_t start_id = log_offset / bm_ -> block_size();
  u8 *buf[DiskBlockSize];
  for (auto &op : ops){
    auto bid = op -> block_id_;
    auto nst = op -> new_block_state_;
    LogEntry(txn_id, bid, nst).flush_to_buffer((u8*)buf);

    // std::cout << "entry size: " << sizeof(LogEntry) << std::endl;
    bm_ -> write_log_entry(log_offset, (u8*)buf, sizeof(LogEntry));
    log_offset += sizeof(LogEntry);
  }
  block_id_t end_id = log_offset / bm_ -> block_size();
  const auto base_block_id = bm_->total_blocks() - bm_ -> get_log_blk_num();

  std::cout << start_id << ": " << end_id << std::endl;
  for (auto i = start_id; i < end_id; i ++)
    bm_->sync(i + base_block_id);

  commit_log(txn_id);

  if(is_checkpoint_enabled_){
    if(log_entry_num >= 100 || log_offset >= DiskBlockSize * 1000){
      checkpoint();
    }
  }

  //why I cannot need to free?
  //delete [] buf;
  std::cout << "ret\n";
  log_mtx.unlock();
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  auto initial_offset = log_offset;
  std::vector<u8> buffer(sizeof(LogEntry));
  LogEntry(txn_id, 0xFFFFFFFFFFFFFFFF).flush_to_buffer(buffer.data());
  bm_ -> write_log_entry(log_offset, buffer.data(), sizeof(LogEntry));
  log_offset += sizeof(LogEntry);

  auto start_id = initial_offset / bm_->block_size();
  auto end_id = log_offset / bm_->block_size();
  const auto base_block_id = bm_->total_blocks() - bm_ -> get_log_blk_num();
  for (auto i = start_id; i < end_id; ++i){
    bm_->sync(i + base_block_id);
  }
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  auto log_start = bm_->get_log_start();
  memset(log_start, 0, DiskBlockSize * 1024);
  log_offset = 0;
  log_entry_num = 0;

  bm_->flush();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  u8* log_start = bm_ -> get_log_start();
  u8* log_end = bm_ -> get_log_end();
  auto it = log_start;
  while(it < log_end){
    auto log_entry_ptr = reinterpret_cast<LogEntry *>(it);
    if(log_entry_ptr -> txn_id == 0){
      std::cout << "break\n";
      break;
    }
    
    std::cout << "recover id: " << log_entry_ptr -> txn_id << "blk: " << log_entry_ptr -> block_id << " data: " << log_entry_ptr -> new_block_state << std::endl;
    auto num_entry_tx = 0;
    auto this_txn_id = log_entry_ptr -> txn_id;
    bool is_this_tx_committed = false;
    auto txn_it = it;
    while(txn_it < log_end){
      num_entry_tx ++;
      auto log_entry_ptr = reinterpret_cast<LogEntry *>(txn_it);
      if(log_entry_ptr->txn_id != this_txn_id){
        break;
      }
      if(log_entry_ptr -> block_id == 0xFFFFFFFFFFFFFFFF){
        is_this_tx_committed = true;
        break;
      }
      txn_it += sizeof(LogEntry);
    }
    if(is_this_tx_committed){
      auto redo_it = it;
      while(redo_it < txn_it){
        auto log_entry_ptr = reinterpret_cast<LogEntry *>(redo_it);
        auto block_id = log_entry_ptr -> block_id;
        auto new_block_state = log_entry_ptr -> new_block_state;
        std::vector<u8> buffer(DiskBlockSize);
        memcpy(buffer.data(), new_block_state, DiskBlockSize);
        bm_ -> write_block(block_id, buffer.data());
        redo_it += sizeof(LogEntry);
      }
    }
    it += sizeof(LogEntry) * num_entry_tx;
  }
}
}; // namespace chfs