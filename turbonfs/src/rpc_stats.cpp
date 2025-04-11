#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "rpc_stats.h"
#include "rpc_task.h"
#include "nfs_client.h"

namespace aznfsc {

/* static */ struct rpc_opstat rpc_stats_az::opstats[FUSE_OPCODE_MAX + 1];
/* static */ std::mutex rpc_stats_az::stats_lock_42;
/* static */ std::atomic<uint64_t> rpc_stats_az::app_read_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::server_read_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::failed_read_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::zero_reads = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::app_bytes_read = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::server_bytes_read = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::bytes_read_from_cache = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::bytes_zeroed_from_cache = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::bytes_read_ahead = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::num_readhead = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::tot_getattr_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::getattr_served_from_cache = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::tot_lookup_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::lookup_served_from_cache = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::app_write_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::server_write_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::failed_write_reqs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::app_bytes_written = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::server_bytes_written = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::inline_writes = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::inline_writes_lp = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::inline_writes_gp = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::flush_seq = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::flush_lp = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::flush_gp = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::commit_lp = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::commit_gp = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::writes_np = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::num_sync_membufs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::tot_bytes_sync_membufs = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::rpc_tasks_allocated = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::fuse_responses_awaited = 0;
/* static */ std::atomic<uint64_t> rpc_stats_az::fuse_reply_failed = 0;

/* static */
void rpc_stats_az::dump_stats()
{
    AZLogInfo("Entered dump stats");
}

}
