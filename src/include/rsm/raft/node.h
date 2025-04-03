#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdarg.h>
#include <thread>
#include <unistd.h>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

#include <random>

namespace chfs {

enum class RaftRole { Follower, Candidate, Leader };

struct RaftNodeConfig {
  int node_id;
  uint16_t port;
  std::string ip_address;
};

template <typename StateMachine, typename Command> class RaftNode {

#define RAFT_LOG(fmt, args...)                                                 \
  do {                                                                         \
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(          \
                   std::chrono::system_clock::now().time_since_epoch())        \
                   .count();                                                   \
    char buf[512];                                                             \
    sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now,       \
            __FILE__, __LINE__, my_id, current_term, role, ##args);            \
    thread_pool->enqueue([=]() { std::cerr << buf; });                         \
  } while (0);

public:
  RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
  ~RaftNode();

  /* interfaces for test */
  void set_network(std::map<int, bool> &network_availablility);
  void set_reliable(bool flag);
  int get_list_state_log_num();
  int rpc_count();
  std::vector<u8> get_snapshot_direct();

private:
  /*
   * Start the raft node.
   * Please make sure all of the rpc request handlers have been registered
   * before this method.
   */
  auto start() -> int;

  /*
   * Stop the raft node.
   */
  auto stop() -> int;

  /* Returns whether this node is the leader, you should also return the current
   * term. */
  auto is_leader() -> std::tuple<bool, int>;

  /* Checks whether the node is stopped */
  auto is_stopped() -> bool;

  /*
   * Send a new command to the raft nodes.
   * The returned tuple of the method contains three values:
   * 1. bool:  True if this raft node is the leader that successfully appends
   * the log, false If this node is not the leader.
   * 2. int: Current term.
   * 3. int: Log index.
   */
  auto new_command(std::vector<u8> cmd_data, int cmd_size)
      -> std::tuple<bool, int, int>;

  /* Save a snapshot of the state machine and compact the log. */
  auto save_snapshot() -> bool;

  /* Get a snapshot of the state machine */
  auto get_snapshot() -> std::vector<u8>;

  /* Internal RPC handlers */
  auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
  auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
  auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

  /* RPC helpers */
  void send_request_vote(int target, RequestVoteArgs arg);
  void handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                 const RequestVoteReply reply);

  void send_append_entries(int target, AppendEntriesArgs<Command> arg);
  void handle_append_entries_reply(int target,
                                   const AppendEntriesArgs<Command> arg,
                                   const AppendEntriesReply reply);

  void send_install_snapshot(int target, InstallSnapshotArgs arg);
  void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                     const InstallSnapshotReply reply);

  /* background workers */
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  /* Data structures */
  bool network_stat; /* for test */

  std::mutex mtx;         /* A big lock to protect the whole data structure. */
  std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<RaftLog<StateMachine, Command>>
      log_storage;                     /* To persist the raft log. */
  std::unique_ptr<StateMachine> state; /*  The state machine that applies the
                                          raft log, e.g. a kv store. */

  std::unique_ptr<RpcServer>
      rpc_server; /* RPC server to recieve and handle the RPC requests. */
  std::map<int, std::unique_ptr<RpcClient>>
      rpc_clients_map; /* RPC clients of all raft nodes including this node. */
  std::vector<RaftNodeConfig> node_configs; /* Configuration for all nodes */
  int my_id; /* The index of this node in rpc_clients, start from 0. */

  std::atomic_bool stopped;

  RaftRole role;
  int current_term;
  int leader_id; 

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;

  /* Lab3: Your code here */
  std::vector<LogEntry<Command>> log_list;
  int received_votes;
  int commit_index;
  int last_applied;
  std::vector<int> next_index;
  std::vector<int> match_index;
  unsigned long last_received_ts;
  unsigned long election_start_time;
  unsigned long time_out_election;
  unsigned long time_out_heartbeat;
  std::shared_ptr<BlockManager> bm;

  auto get_time() -> unsigned long;
  auto restart_random_time_out(int max, int min) -> unsigned long;
  auto logic2phy(int logic) -> int;
  auto phy2logic(int phy) -> int;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
    : network_stat(true), node_configs(configs), my_id(node_id), stopped(true),
      role(RaftRole::Follower), current_term(0), leader_id(-1) {
  auto my_config = node_configs[my_id];

  /* launch RPC server */
  rpc_server =
      std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

  /* Register the RPCs. */
  rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
  rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
  rpc_server->bind(RAFT_RPC_CHECK_LEADER,
                   [this]() { return this->is_leader(); });
  rpc_server->bind(RAFT_RPC_IS_STOPPED,
                   [this]() { return this->is_stopped(); });
  rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                   [this](std::vector<u8> data, int cmd_size) {
                     return this->new_command(data, cmd_size);
                   });
  rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT,
                   [this]() { return this->save_snapshot(); });
  rpc_server->bind(RAFT_RPC_GET_SNAPSHOT,
                   [this]() { return this->get_snapshot(); });

  rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) {
    return this->request_vote(arg);
  });
  rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) {
    return this->append_entries(arg);
  });
  rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) {
    return this->install_snapshot(arg);
  });

  /* Lab3: Your code here */
  bm = std::make_shared<BlockManager>("/tmp/raft_log/data" + std::to_string(my_id));
  thread_pool = std::make_unique<ThreadPool>(32);
  log_storage =
      std::make_unique<RaftLog<StateMachine, Command>>(bm);
  state = std::make_unique<StateMachine>();

  if (log_storage -> need_recover()) {
    log_storage -> recover(current_term, leader_id, log_list, state);
    last_applied = log_list[0].logic_index;
    commit_index = log_list[0].logic_index;
  } else {
    LogEntry<Command> entry;
    entry.logic_index = 0;
    entry.term = 0;
    log_list.push_back(entry);
    commit_index = 0;
    last_applied = 0;
  }

  received_votes = 0;
  next_index.resize(node_configs.size());
  match_index.resize(node_configs.size());
  last_received_ts = get_time();
  election_start_time = get_time();
  time_out_election = restart_random_time_out(300, 150);
  time_out_heartbeat = restart_random_time_out(300, 150);

  rpc_server -> run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
  stop();
  thread_pool.reset();
  rpc_server.reset();
  state.reset();
  log_storage.reset();

  /* Lab3: Your code here */
  bm.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_time() -> unsigned long {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::restart_random_time_out(int max, int min)
    -> unsigned long {
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator(seed);
  std::uniform_int_distribution<unsigned long> distribution(min, max);
  int result = distribution(generator);
  return result;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::logic2phy(int logic) -> int {
  int head_idx = log_list[0].logic_index;
  if (logic < head_idx) {
    return -1;
  }
  return logic - head_idx;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::phy2logic(int phy) -> int {
  if (phy >= 0 && phy < log_list.size()) {
    return log_list[phy].logic_index;
  }
  return -1;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  /* Lab3: Your code here */
  // 初始化rpc map
  rpc_clients_map.clear();
  for (auto &item : node_configs) {
    rpc_clients_map.insert(std::make_pair(
        item.node_id,
        std::make_unique<RpcClient>(item.ip_address, item.port, true)));
  }
  // 启动标志更改
  stopped.store(false);
  // 启动线程创建
  background_election =
      std::make_unique<std::thread>(&RaftNode::run_background_election, this);
  background_ping =
      std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
  background_commit =
      std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
  background_apply =
      std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  /* Lab3: Your code here */
  stopped.store(true);
  background_election -> join();
  background_ping -> join();
  background_commit -> join();
  background_apply -> join();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  std::lock_guard<std::mutex> lock(mtx);
  return std::make_tuple(role == RaftRole::Leader, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
  return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
    -> std::tuple<bool, int, int> {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  if(role == RaftRole::Leader){
    int next_log_index = log_list.back().logic_index + 1;
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);

    LogEntry<Command> log_entry({current_term, next_log_index, cmd});
    log_list.push_back(log_entry);
    log_storage -> persist_log(log_list);
    return std::make_tuple(true, current_term, next_log_index);
  }

  return std::make_tuple(false, current_term, log_list.back().logic_index);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  for (int i = logic2phy(last_applied) + 1; i <= logic2phy(commit_index); i ++) {
    if (i >= 1 && i < log_list.size()) {
      state -> apply_log(log_list[i].content);
    } else {
      RAFT_LOG("Invalid apply");
      return false;
    }
  }
  last_applied = commit_index;
  log_list[0].term = log_list[logic2phy(commit_index)].term;
  log_list[0].logic_index = commit_index;

  auto it = log_list.begin() + 1;
  while(it != log_list.end() && it -> logic_index <= commit_index) {
    it = log_list.erase(it);
  }

  if (role == RaftRole::Leader) {
    for (int i = 0; i < rpc_clients_map.size(); ++i) {
      next_index[i] = commit_index + 1;
    }
  }

  log_storage -> persist_snapshot(log_list[0].logic_index, log_list[0].term, state -> snapshot());
  log_storage -> persist_log(log_list);
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  /* Lab3: Your code here */
  return get_snapshot_direct();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
    -> RequestVoteReply {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  RequestVoteReply reply({current_term, false, my_id});
  bool metadata_change = false;
  /* vote receiver impl 1*/
  if(args.term < current_term)
    return reply;
  
  /* rules for all 2 */
  if(args.term > current_term){
    current_term = args.term;
    role = RaftRole::Follower;
    metadata_change = true;
    leader_id = -1;
    time_out_heartbeat = restart_random_time_out(300, 150);
    last_received_ts = get_time();
  }
  /* vote receiver impl 2*/
  bool log_complete_bo = !((log_list.back().term > args.lastLogTerm) ||
        ((log_list.back().term == args.lastLogTerm) && (log_list.back().logic_index > args.lastLogIndex)));
  if(args.term == current_term && (leader_id == -1 || leader_id == args.candidateId) && log_complete_bo){
    last_received_ts = get_time();
    leader_id = args.candidateId;
    reply.voteGranted = true;
    role = RaftRole::Follower;
  }

  if (metadata_change) {
    log_storage -> persist_metadata(current_term, leader_id);
  }

  return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    int target, const RequestVoteArgs args, const RequestVoteReply reply) {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  bool metadata_change = false;
  if(role != RaftRole::Candidate || args.term < current_term)
    return;

  /* rules for all 2 */
  if(args.term > current_term){
    current_term = args.term;
    role = RaftRole::Follower;
    metadata_change = true;
    leader_id = -1;
    time_out_heartbeat = restart_random_time_out(300, 150);
    last_received_ts = get_time();
  }
  /* candidate impl 2*/
  if(role == RaftRole::Candidate && reply.voteGranted){
    received_votes ++;
    if(received_votes >= rpc_clients_map.size() / 2 + 1){
      RAFT_LOG("I become leader");
      role = RaftRole::Leader;
      int init_next_index = log_list.back().logic_index + 1;
      for (int i = 0; i < rpc_clients_map.size(); ++i) {
        next_index[i] = init_next_index;
        match_index[i] = 0;
      }
    }
  }
  if (metadata_change) {
    log_storage -> persist_metadata(current_term, leader_id);
  }

  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
  /* Lab3: Your code here */
  AppendEntriesReply result({current_term, false});
  AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);
  std::unique_lock<std::mutex> lock(mtx);
  bool metadata_change = true;

  /* receiver impl 1*/
  if(arg.leaderTerm < current_term)
    return result;

  if (arg.leaderTerm >= current_term) {
    current_term = arg.leaderTerm;
    role = RaftRole::Follower;
    received_votes = 0;
    leader_id = arg.leaderId;
    metadata_change = true;
    time_out_heartbeat = restart_random_time_out(300, 150);
    last_received_ts = get_time();
  }

  if (current_term == arg.leaderTerm) {
    int last_new_entry_index;
    last_received_ts = get_time();

    /* receiver impl 2*/
    int phy_log_index = logic2phy(arg.prevLogIndex);
    if (phy_log_index == -1 || phy_log_index >= log_list.size() || log_list[phy_log_index].term != arg.prevLogTerm) {
      if (metadata_change) {
        log_storage -> persist_metadata(current_term, leader_id);
      }
      return result;
    }
    result.success = true;
    last_new_entry_index = log_list[phy_log_index].logic_index;

    if (!arg.entries.empty()) {
      phy_log_index += 1;
      /* receiver impl 3, 4 */
      for (int i = 0; i < arg.entries.size(); ++i) {
        if (phy_log_index + i >= log_list.size()) {
          last_new_entry_index ++;
          LogEntry<Command> entry({arg.leaderTerm, last_new_entry_index, arg.entries[i]});
          log_list.push_back(entry);
        } else {
          log_list[i + phy_log_index].term = arg.leaderTerm;
          log_list[i + phy_log_index].content = arg.entries[i];
          last_new_entry_index ++;
        }
      }
      log_storage -> persist_log(log_list);
    }
    /* receiver impl 5 */
    if (arg.leaderCommit > commit_index) {
      commit_index = std::min(arg.leaderCommit, last_new_entry_index);
    }
  }
  if (metadata_change) {
    log_storage -> persist_metadata(current_term, leader_id);
  }
  return result;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    int node_id, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  bool metadata_change = false;

  if (reply.term > current_term) {
    current_term = reply.term;
    role = RaftRole::Follower;
    leader_id = -1;
    metadata_change = true;
    time_out_heartbeat = restart_random_time_out(300, 150);
    last_received_ts = get_time();
  }

  if (role == RaftRole::Leader) {
    if (reply.success) {
      /* leader impl 3.1 */
      if (!arg.entries.empty()) {
        match_index[node_id] = arg.entries.size() + arg.prevLogIndex;
        next_index[node_id] = match_index[node_id] + 1;

        /* leader impl 4*/
        for (int i = logic2phy(commit_index + 1); i < log_list.size(); i ++) {
          int cnt = 0;
          if (log_list[i].term != current_term)
            continue;

          int logic_log_index = phy2logic(i);
          for(auto &client_pair: rpc_clients_map){
            if(client_pair.first == my_id || match_index[client_pair.first] >= logic_log_index)
              cnt ++;
          }
          if (cnt >= (rpc_clients_map.size() / 2) + 1) 
            commit_index = logic_log_index;
        }
      }
    } else {
      /* leader impl 3.2 */
      if (logic2phy(next_index[node_id]) == 1) {
        InstallSnapshotArgs arg({current_term, my_id, log_list[0].logic_index, log_list[0].term, state -> snapshot()});
        thread_pool -> enqueue(&RaftNode::send_install_snapshot, this, node_id, arg);
      } else {
        next_index[node_id] --;
      }
    }
  }
  if (metadata_change) {
    log_storage -> persist_metadata(current_term, leader_id);
  }
  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
    -> InstallSnapshotReply {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);

  if (args.leaderTerm >= current_term) {
    current_term = args.leaderTerm;
    role = RaftRole::Follower;
    received_votes = 0;
    leader_id = args.leaderId;
    log_storage -> persist_metadata(current_term, leader_id);
    time_out_heartbeat = restart_random_time_out(300, 150);
    last_received_ts = get_time();
  }
  if (current_term == args.leaderTerm) {
    last_received_ts = get_time();
  }

  if (current_term == args.leaderTerm && args.lastIncludeIndex > log_list[0].logic_index) {
    state -> apply_snapshot(args.snapshot_data);

    log_list[0].term = args.lastIncludeTerm;
    log_list[0].logic_index = args.lastIncludeIndex;

    auto it = log_list.begin() + 1;
    while( it != log_list.end() && it -> logic_index <= args.lastIncludeIndex) {
      it = log_list.erase(it);
    }
    last_applied = args.lastIncludeIndex;
    commit_index = std::max(args.lastIncludeIndex, commit_index);

    log_storage -> persist_snapshot(log_list[0].logic_index, log_list[0].term, state -> snapshot());
    log_storage -> persist_metadata(current_term, leader_id);
  }
  return InstallSnapshotReply({current_term});
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  if (reply.term > current_term) {
    current_term = reply.term;
    role = RaftRole::Follower;
    leader_id = -1;
    log_storage -> persist_metadata(current_term, leader_id);
    time_out_heartbeat = restart_random_time_out(300, 150);
    last_received_ts = get_time();
  }

  if (role == RaftRole::Leader) {
    match_index[node_id] = std::max(arg.lastIncludeIndex, match_index[node_id]);
    next_index[node_id] = match_index[node_id] + 1;
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id,
                                                        RequestVoteArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id] -> get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id] -> call(RAFT_RPC_REQUEST_VOTE, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_request_vote_reply(target_id, arg,
                              res.unwrap() -> as<RequestVoteReply>());
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(
    int target_id, AppendEntriesArgs<Command> arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id] -> get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  auto res = rpc_clients_map[target_id] -> call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_append_entries_reply(target_id, arg, res.unwrap() -> as<AppendEntriesReply>());
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(
    int target_id, InstallSnapshotArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id] -> get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id] -> call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_install_snapshot_reply(target_id, arg,
                                  res.unwrap() -> as<InstallSnapshotReply>());
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
  // Periodly check the liveness of the leader.

  // Work for followers and candidates.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
          return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      bool metadata_change = false;
      bool follow_over = ((role == RaftRole::Follower) &&
           ((get_time() - last_received_ts) > time_out_heartbeat));
      bool candidate_over = ((role == RaftRole::Candidate) &&
                             ((get_time() - election_start_time) > time_out_election));
      
      /* follower impl 2 -> candidate impl 1*/
      /* candidate impl 4 */
      if(follow_over || candidate_over){
        if(role == RaftRole::Follower){
          role = RaftRole::Candidate;
          RAFT_LOG("I become candidate");
        }
        ++ current_term;
        received_votes = 1;
        leader_id = my_id;

        election_start_time = get_time();
        time_out_election = restart_random_time_out(300, 150);
        for(auto &client_pair: rpc_clients_map){
          if(client_pair.first == my_id) continue;
          RequestVoteArgs request({current_term, my_id, log_list.back().logic_index, log_list.back().term});
          thread_pool -> enqueue(&RaftNode::send_request_vote, this, client_pair.first, request);
        }
      }
      if (metadata_change) {
        log_storage -> persist_metadata(current_term, leader_id);
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
  // Periodly send logs to the follower.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      if (role == RaftRole::Leader) {
        for(auto &client_pair: rpc_clients_map){
          if(client_pair.first == my_id) continue;
          int start_idx = logic2phy(next_index[client_pair.first] - 1);
          
          std::vector<Command> entries;
          for (int i = start_idx + 1; i < log_list.size(); i ++) {
            entries.push_back(log_list[i].content);
          }
          if (entries.empty())
            continue;
          
          AppendEntriesArgs<Command> arg({
            current_term, my_id, next_index[client_pair.first] - 1,
            log_list[start_idx].term, commit_index, entries
          });
          thread_pool -> enqueue(&RaftNode::send_append_entries, this, client_pair.first, arg);
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  // Work for all the nodes.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      for (int phy_log_idx = logic2phy(last_applied + 1), i = last_applied + 1;
           i <= commit_index; ++phy_log_idx, ++i) {
        if (phy_log_idx >= 1 && phy_log_idx < log_list.size()) {
          state -> apply_log(log_list[phy_log_idx].content);
          last_applied ++;
        } else {
          RAFT_LOG("Invalid apply");
          break;
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);

      if (role == RaftRole::Leader) {
        for(auto &client_pair: rpc_clients_map){
          if(client_pair.first == my_id) continue;
          AppendEntriesArgs<Command> args({
            current_term, my_id, next_index[client_pair.first] - 1,
            log_list[logic2phy(next_index[client_pair.first] - 1)].term,
            commit_index, std::vector<Command>()
          });
          thread_pool -> enqueue(&RaftNode::send_append_entries, this, client_pair.first, args);
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(
    std::map<int, bool> &network_availability) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  /* turn off network */
  if (!network_availability[my_id]) {
    for (auto &&client : rpc_clients_map) {
      if (client.second != nullptr)
        client.second.reset();
    }

    return;
  }

  for (auto node_network : network_availability) {
    int node_id = node_network.first;
    bool node_status = node_network.second;

    if (node_status && rpc_clients_map[node_id] == nullptr) {
      RaftNodeConfig target_config;
      for (auto config : node_configs) {
        if (config.node_id == node_id)
          target_config = config;
      }

      rpc_clients_map[node_id] = std::make_unique<RpcClient>(
          target_config.ip_address, target_config.port, true);
    }

    if (!node_status && rpc_clients_map[node_id] != nullptr) {
      rpc_clients_map[node_id].reset();
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      client.second -> set_reliable(flag);
    }
  }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
  /* only applied to ListStateMachine*/
  std::unique_lock<std::mutex> lock(mtx);

  return state -> num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
  int sum = 0;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      sum += client.second -> count();
    }
  }

  return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
  if (is_stopped()) {
    return std::vector<u8>();
  }

  std::unique_lock<std::mutex> lock(mtx);

  return state -> snapshot();
}

} // namespace chfs