#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include <cstring>
#include <mutex>
#include <vector>

namespace chfs {

template <typename content_type> struct LogEntry {
  int term = 0;
  int logic_index = 0;
  content_type content;

  int serialize_size() {
    return sizeof(term) + sizeof(logic_index) + content.size();
  }

  void serialize(std::vector<u8> &data) {
    data.resize(serialize_size());
    *(reinterpret_cast<int *>(data.data())) = term;
    *(reinterpret_cast<int *>(data.data() + sizeof(int))) = logic_index;
    std::vector<u8> content_data = content.serialize(content.size());
    memcpy((data.data() + 8), content_data.data(), content.size());
  }

  void deserialize(std::vector<u8> &data) {
    if (data.size() == serialize_size()) {
      term = *(reinterpret_cast<int *>(data.data()));
      logic_index = *(reinterpret_cast<int *>(data.data() + sizeof(int)));
      std::vector<u8> content_data(data.begin() + 8, data.end());
      content.deserialize(content_data, content.size());
    }
  }
};

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename StateMachine, typename Command> class RaftLog {
public:
  ~RaftLog();
  RaftLog(std::shared_ptr<BlockManager> bm);
  /* Lab3: Your code here */
  bool persist_metadata(int current_term, int voted_for);
  bool persist_log(std::vector<LogEntry<Command>> &log_list);
  bool persist_snapshot(int last_include_index, int last_include_term,
                        std::vector<u8> snapshot_data);
  bool recover(int &current_term, int &voted_for,
               std::vector<LogEntry<Command>> &log_list,
               std::unique_ptr<StateMachine> &state);
  bool need_recover();

private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
  int log_num_per_block;
  int log_ser_size;

  int valid; 
  int current_term;
  int voted_for;
  int snapshot_start_block;
  int snapshot_end_block;
  int log_start_block;
  int log_num;
  int snapshot_data_size;
  int snapshot_last_index;
  int snapshot_last_term;

  const int valid_off;
  const int current_term_off;
  const int voted_for_off;
  const int snapshot_start_block_off;
  const int snapshot_end_block_off;
  const int log_start_block_off;
  const int log_num_off;
  const int snapshot_data_size_off;
  const int snapshot_last_index_off;
  const int snapshot_last_term_off;

  inline int get_int(std::vector<u8> &data, int off);
  inline void flush_int(std::vector<u8> &dst, int src, int off);
};

template <typename StateMachine, typename Command>
RaftLog<StateMachine, Command>::RaftLog(std::shared_ptr<BlockManager> bm) :
  valid_off(0),
  current_term_off(4),
  voted_for_off(8),
  snapshot_start_block_off(12),
  snapshot_end_block_off(16),
  log_start_block_off(20),
  log_num_off(24),
  snapshot_data_size_off(28),
  snapshot_last_index_off(32),
  snapshot_last_term_off(36) {
  /* Lab3: Your code here */
  bm_ = bm;
  
  std::vector<u8> buf(bm_ -> block_size());
  bm_ -> read_block(0, buf.data());

  valid = get_int(buf, valid_off);
  if (valid == 1) {
    current_term = get_int(buf, current_term_off);
    voted_for = get_int(buf, voted_for_off);
    snapshot_start_block = get_int(buf, snapshot_start_block_off);
    snapshot_end_block = get_int(buf, snapshot_end_block_off);
    log_start_block = get_int(buf, log_start_block_off);
    log_num = get_int(buf, log_num_off);
    snapshot_data_size = get_int(buf, snapshot_data_size_off);
    snapshot_last_index = get_int(buf, snapshot_last_index_off);
    snapshot_last_term = get_int(buf, snapshot_last_term_off);
  } else {
    valid = 1;
    current_term = 0;
    voted_for = -1;
    snapshot_start_block = 1;
    snapshot_end_block = 2;
    log_start_block = 2;
    log_num = 0;
    snapshot_data_size = 0;
    snapshot_last_index = 0;
    snapshot_last_term = 0;
    flush_int(buf, valid, valid_off);
    flush_int(buf, current_term, current_term_off);
    flush_int(buf, voted_for, voted_for_off);
    flush_int(buf, snapshot_start_block, snapshot_start_block_off);
    flush_int(buf, snapshot_end_block, snapshot_end_block_off);
    flush_int(buf, log_start_block, log_start_block_off);
    flush_int(buf, log_num, log_num_off);
    flush_int(buf, snapshot_data_size, snapshot_data_size_off);
    flush_int(buf, snapshot_last_index, snapshot_last_index_off);
    flush_int(buf, snapshot_last_term, snapshot_last_term_off);
    bm_ -> write_block(0, buf.data());
    bm_ -> sync(0);
  }

  log_ser_size = LogEntry<Command>().serialize_size();
  log_num_per_block = bm_ -> block_size() / log_ser_size;
}

template <typename StateMachine, typename Command>
RaftLog<StateMachine, Command>::~RaftLog() {
  /* Lab3: Your code here */
  bm_.reset();
}

/* Lab3: Your code here */
template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::persist_metadata(int current_term, int voted_for) {
  std::unique_lock<std::mutex> lock(mtx);
  bm_ -> write_partial_block(0, reinterpret_cast<u8 *>(&current_term), current_term_off, sizeof(int));
  bm_ -> write_partial_block(0, reinterpret_cast<u8 *>(&voted_for), voted_for_off, sizeof(int));
  bm_ -> sync(0);
  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::persist_log(std::vector<LogEntry<Command>> &log_list) {
  std::unique_lock<std::mutex> lock(mtx);
  int cur_blk_id = log_start_block, cur_off = 0;

  for (auto it = log_list.begin() + 1; it != log_list.end(); it ++) {
    std::vector<u8> buf;
    it -> serialize(buf);
    bm_ -> write_partial_block(cur_blk_id, buf.data(), cur_off * (log_ser_size), log_ser_size);
    cur_off ++;
    if (cur_off == log_num_per_block) {
      cur_off = 0;
      cur_blk_id ++;
    }
  }
  for (int i = log_start_block; i <= cur_blk_id; ++i) {
    bm_ -> sync(i);
  }

  log_num = log_list.size() - 1;
  bm_ -> write_partial_block(0, reinterpret_cast<u8 *>(&log_num), log_num_off, sizeof(int));
  bm_ -> sync(0);
  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::persist_snapshot(int last_include_index, int last_include_term, std::vector<u8> snapshot_data) {
  std::unique_lock<std::mutex> lock(mtx);

  snapshot_last_index = last_include_index;
  snapshot_last_term = last_include_term;
  snapshot_data_size = snapshot_data.size();
  bm_ -> write_partial_block(0, reinterpret_cast<u8 *>(&snapshot_last_index), snapshot_last_index_off, sizeof(int));
  bm_ -> write_partial_block(0, reinterpret_cast<u8 *>(&snapshot_last_term), snapshot_last_term_off, sizeof(int));
  bm_ -> write_partial_block(0, reinterpret_cast<u8 *>(&snapshot_data_size), snapshot_data_size_off, sizeof(int));
  bm_ -> sync(0);

  int left_data_size = snapshot_data.size();
  int offset = 0;
  int cur_blk = snapshot_start_block;
  while (left_data_size > 0){
    int chunk_size = (left_data_size >= bm_ -> block_size()) ? bm_ -> block_size() : left_data_size;
    bm_ -> write_partial_block(cur_blk, (snapshot_data.data() + offset), 0, chunk_size);
    bm_ -> sync(cur_blk);

    cur_blk ++;
    left_data_size -= chunk_size;
    offset += chunk_size;
  }
  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::recover(int &current_term, int &voted_for, std::vector<LogEntry<Command>> &log_list, std::unique_ptr<StateMachine> &state) {
  std::unique_lock<std::mutex> lock(mtx);
  current_term = this -> current_term;
  voted_for = this -> voted_for;

  if (snapshot_data_size > 0) {
    std::vector<u8> snapshot_data;
    snapshot_data.reserve(snapshot_data_size);
    int left_data_size = snapshot_data_size;
    int cur_blk = snapshot_start_block;
    while (left_data_size > 0){
      int chunk_size = (left_data_size >= bm_ -> block_size()) ? bm_ -> block_size() : left_data_size;
      std::vector<u8> page_data(bm_ -> block_size());
      bm_ -> read_block(cur_blk, page_data.data());
      snapshot_data.insert(snapshot_data.end(), page_data.begin(), page_data.begin() + chunk_size);

      cur_blk ++;
      left_data_size -= chunk_size;
    }
    state -> apply_snapshot(snapshot_data);
  }

  LogEntry<Command> entry;
  entry.logic_index = snapshot_last_index;
  entry.term = snapshot_last_term;
  log_list.push_back(entry);

  int cur_blk_id = log_start_block;
  int cur_off = 0;
  std::vector<u8> page_data(bm_ -> block_size());
  bm_ -> read_block(cur_blk_id, page_data.data());
  for (int i = 0; i < log_num; ++i) {
    std::vector<u8> buf(page_data.begin() + (cur_off * log_ser_size), page_data.begin() + ((cur_off + 1) * log_ser_size));
    entry.deserialize(buf);
    log_list.push_back(entry);

    cur_off ++;
    if (cur_off == log_num_per_block) {
      cur_off = 0;
      cur_blk_id ++;
      bm_ -> read_block(cur_blk_id, page_data.data());
    }
  }

  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::need_recover() {
  std::unique_lock<std::mutex> lock(mtx);
  return valid == 1;
}

template <typename StateMachine, typename Command>
inline int RaftLog<StateMachine, Command>::get_int(std::vector<u8> &data, int off) {
  return *(reinterpret_cast<int *>(data.data() + off));
}

template <typename StateMachine, typename Command>
inline void RaftLog<StateMachine, Command>::flush_int(std::vector<u8> &dst, int src, int off) {
  memcpy(reinterpret_cast<char *>(dst.data() + off), reinterpret_cast<char *>(&src), sizeof(int));
}

} /* namespace chfs */
