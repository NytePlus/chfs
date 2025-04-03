#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"
#include <vector>

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;

    MSGPACK_DEFINE(
      term,
      candidateId,
      lastLogIndex,
      lastLogTerm
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int term;
    bool voteGranted;
    int followerId;

    MSGPACK_DEFINE(
      term,
      voteGranted,
      followerId
    )
};

template <typename Command> struct AppendEntriesArgs {
  /* Lab3: Your code here */
  int leaderTerm;
  int leaderId;
  int prevLogIndex;
  int prevLogTerm;
  int leaderCommit;
  std::vector<Command> entries;
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  int leaderTerm;
  int leaderId;
  int prevLogIndex;
  int prevLogTerm;
  int leaderCommit;
  std::vector<u8> entries;
  MSGPACK_DEFINE(
    leaderTerm, 
    leaderId, 
    prevLogIndex, 
    prevLogTerm,
    leaderCommit, 
    entries
  )
};

template <typename Command>
RpcAppendEntriesArgs
transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  std::vector<u8> buf;
  for (Command item : arg.entries) {
    std::vector<u8> ser_item = item.serialize(item.size());
    for (int i = 0; i < ser_item.size(); i ++)
      buf.push_back(ser_item[i]);
  }
  return RpcAppendEntriesArgs({arg.leaderTerm, arg.leaderId, arg.prevLogIndex, arg.prevLogTerm, arg.leaderCommit, buf});
}

template <typename Command>
AppendEntriesArgs<Command>
transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  std::vector<Command> entries;
  std::vector<u8> data;
  int current_length = 0, require_length = Command().size();
  for (u8 char_item : rpc_arg.entries) {
    data.push_back(char_item);
    current_length ++;
    if (current_length == require_length) {
      Command tmp;
      tmp.deserialize(data, require_length);
      entries.push_back(tmp);
      current_length = 0;
      data.clear();
    }
  }
  return AppendEntriesArgs<Command>({rpc_arg.leaderTerm, rpc_arg.leaderId, rpc_arg.prevLogIndex, rpc_arg.prevLogTerm, rpc_arg.leaderCommit, entries});
}

struct AppendEntriesReply {
  /* Lab3: Your code here */
  int term;
  int success;
  MSGPACK_DEFINE(
    term, 
    success
  )
};

struct InstallSnapshotArgs {
  /* Lab3: Your code here */
  int leaderTerm;
  int leaderId;
  int lastIncludeIndex;
  int lastIncludeTerm;
  std::vector<u8> snapshot_data;
  MSGPACK_DEFINE(
    leaderTerm, 
    leaderId, 
    lastIncludeIndex, 
    lastIncludeTerm,
    snapshot_data
  )
};

struct InstallSnapshotReply {
  /* Lab3: Your code here */
  int term;
  MSGPACK_DEFINE(term)
};

} /* namespace chfs */