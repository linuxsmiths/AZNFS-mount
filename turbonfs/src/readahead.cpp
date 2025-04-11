#include "aznfsc.h"
#include "readahead.h"
#include "rpc_task.h"
#include "file_cache.h"

/*
 * This enables debug logs and also runs the self tests.
 * Must enable once after adding a new self-test or making any changes to
 * the class.
 */
//#define DEBUG_READAHEAD

#define _MiB (1024 * 1024LL)
#define _GiB (_MiB * 1024)
#define _TiB (_GiB * 1024)

#define NFS_STATUS(r) ((r) ? (r)->status : NFS3ERR_SERVERFAULT)

namespace aznfsc {

/**
 * This is called from alloc_rastate() with exclusive lock on ilock_1.
 */
ra_state::ra_state(struct nfs_client *_client,
                   struct nfs_inode *_inode) :
        client(_client),
        inode(_inode),
        ra_bytes(client->mnt_options.readahead_kb * 1024),
        def_ra_size(std::min<uint64_t>(client->mnt_options.rsize_adj, ra_bytes))
{
    assert(client->magic == NFS_CLIENT_MAGIC);
    assert(inode->magic == NFS_INODE_MAGIC);

    // We should be called only for regular files.
    assert(inode->is_regfile());

    /*
     * Readaheads are triggered by application reads and application reads
     * are only performed on an open fd and we create filecache when file is
     * opened by fuse.
     * XXX We cannot perform the following assert as our caller alloc_rastate()
     *     is already holding ilock_1, but note that alloc_rastate() does this
     *     assert.
     */
#if 0
    assert(inode->has_filecache());
#endif

    /*
     * By the time ra_state is initialized mount must have already
     * completed and we must have the rsize/wsize value advertized
     * by the server.
     */
    assert(client->mnt_options.rsize_adj >= AZNFSCFG_RSIZE_MIN &&
           client->mnt_options.rsize_adj <= AZNFSCFG_RSIZE_MAX);

    assert((client->mnt_options.readahead_kb >= AZNFSCFG_READAHEAD_KB_MIN &&
            client->mnt_options.readahead_kb <= AZNFSCFG_READAHEAD_KB_MAX) ||
           (client->mnt_options.readahead_kb == 0));

    AZLogDebug("[{}] Readahead set to {} bytes with default RA size {} bytes",
               inode->get_fuse_ino(), ra_bytes, def_ra_size);
}

uint64_t ra_state::get_ra_bytes() const
{
    return ra_bytes * nfs_client::get_ra_scale_factor();
}

/**
 * Readahead context.
 * All ongoing readahead reads are tracked using one ra_context object.
 */
struct ra_context
{
    /*
     * bytes_chunk which this readahead is reading from the file.
     */
    struct bytes_chunk bc;

    /*
     * rpc_task tracking this readahead.
     *
     * Note: We don't strictly need an rpc_task to track readahead reads
     *       since we don't need to send a fuse reply, but we still use
     *       one rpc_task per readahead read so that readahead reads are
     *       also limited by the number of concurrent rpc_tasks allowed.
     */
    struct rpc_task *task;

    ra_context(rpc_task *_task, struct bytes_chunk& _bc) :
        bc(_bc),
        task(_task)
    {
        assert(task->magic == RPC_TASK_MAGIC);
        assert(bc.length > 0 && bc.length <= AZNFSC_MAX_CHUNK_SIZE);
        assert(bc.offset < AZNFSC_MAX_FILE_SIZE);
    }
};

int64_t ra_state::get_next_ra(uint64_t length)
{
    if (length == 0) {
        length = def_ra_size;
    }

    /*
     * RA is disabled?
     */
    if (length == 0) {
        return -1;
    }

    /*
     * Don't perform readahead beyond eof.
     * If we don't have a file size estimate (probably the attr cache is too
     * old) then also we play safe and do not perform readahead.
     */
    const int64_t filesize =
        inode ? inode->get_server_file_size(): AZNFSC_MAX_FILE_SIZE;
    assert(filesize >= 0 || filesize == -1);
    if ((filesize == -1) ||
        ((int64_t) (last_byte_readahead + 1 + length) > filesize)) {
        return -2;
    }

    /*
     * Application read pattern is known to be non-sequential?
     */
    if (!is_sequential()) {
        return -3;
    }

    /*
     * Scaled ra_bytes is the ra_bytes scaled to account for global cache
     * pressure. We use that to decide how much to readahead.
     */
    const uint64_t ra_bytes_scaled = get_ra_bytes();

    /*
     * If we already have ra_bytes readahead bytes read, don't readahead
     * more.
     */
    if ((last_byte_readahead + length) > (max_byte_read + ra_bytes_scaled)) {
        return -4;
    }

    /*
     * Keep readahead bytes issued always less than the scaled ra_bytes.
     */
    if ((ra_ongoing += length) > ra_bytes_scaled) {
        assert(ra_ongoing >= length);
        ra_ongoing -= length;
        return -5;
    }

    std::unique_lock<std::shared_mutex> _lock(ra_lock_40);

    /*
     * Atomically update last_byte_readahead, as we don't want to return
     * duplicate readahead offset to multiple calls.
     */
    const uint64_t next_ra =
        std::atomic_exchange(&last_byte_readahead, last_byte_readahead + length) + 1;

    assert((int64_t) next_ra > 0);
    return next_ra;
}
/*
 * TODO: Add readahead stats.
 */
/**
 * Note: This takes shared lock on ilock_1.
 */
int ra_state::issue_readaheads()
{
   AZLogInfo("Called isse_readaheads");
   return 1;
}

/* static */
int ra_state::unit_test()
{
    ra_state ras{128 * 1024, 4 * 1024};
    int64_t next_ra;
    int64_t next_read;
    int64_t complete_ra;

    AZLogInfo("Unit testing ra_state, start");

    // 1st read.
    next_read = 0*_MiB;
    ras.on_application_read(next_read, 1*_MiB);

    // Only 1 read complete, cannot confirm sequential pattern till 3 reads.
    assert(ras.get_next_ra(4*_MiB) == 0);

    next_read += 1*_MiB;
    ras.on_application_read(next_read, 1*_MiB);

    // Only 2 reads complete, cannot confirm sequential pattern till 3 reads.
    assert(ras.get_next_ra(4*_MiB) == 0);

    next_read += 1*_MiB;
    ras.on_application_read(next_read, 1*_MiB);

    /*
     * Ok 3 reads complete, all were sequential, so now we should get a
     * readahead recommendation.
     */
    next_ra = 3*_MiB;
    assert(ras.get_next_ra(4*_MiB) == next_ra);

    /*
     * Since we have 128MB ra window, next 31 (+1 above) get_next_ra() calls
     * will recommend readahead.
     */
    for (int i = 0; i < 31; i++) {
        next_ra += 4*_MiB;
        /*
         * We don't pass the length parameter to get_next_ra(), it should
         * use the default ra size set in the constructor. We set that to
         * 4MiB.
         */
        assert(ras.get_next_ra() == next_ra);
    }

    // No more readahead reads after full ra window is issued.
    assert(ras.get_next_ra(4*_MiB) == 0);

    /*
     * Complete one readahead.
     * We don't pass the length parameter to on_readahead_complete(), it should
     * use the default ra size set in the constructor. We set that to 4MiB.
     */
    complete_ra = 3*_MiB;
    ras.on_readahead_complete(complete_ra);

    // One more readahead should be allowed.
    next_ra += 4*_MiB;
    assert(ras.get_next_ra(4*_MiB) == next_ra);

    // Not any more.
    assert(ras.get_next_ra(4*_MiB) == 0);

    // Complete all readahead reads.
    for (int i = 0; i < 32; i++) {
        complete_ra += 4*_MiB;
        ras.on_readahead_complete(complete_ra, 4*_MiB);
    }

    // Now it should recommend next readahead.
    next_ra += 4*_MiB;
    assert(ras.get_next_ra(4*_MiB) == next_ra);

    // Complete that one too.
    complete_ra = next_ra;
    ras.on_readahead_complete(complete_ra, 4*_MiB);

    /*
     * Now issue next read at 100MB offset.
     * This will cause access density to drop since now we have a gap of
     * 97MiB and we have just read 4MiB till now.
     */
    ras.on_application_read(100*_MiB, 1*_MiB);
    assert(ras.get_next_ra(4*_MiB) == 0);

    /*
     * Read the entire gap.
     * This will fill the gap and get the access density back to 100%, so
     * now it should recommend readahead.
     */
    for (int i = 0; i < 97; i++) {
        next_read += 1*_MiB;
        ras.on_application_read(next_read, 1*_MiB);
    }

    /*
     * Readahead recommended should be after the last byte read or the last
     * readahead byte, whichever is larger. In this case next readahead is
     * larger.
     */
    next_ra += 4*_MiB;
    assert(next_ra > 101*_MiB);
    assert(ras.get_next_ra(4*_MiB) == next_ra);

    /*
     * Read from a new section.
     * This should reset the pattern detector and it should not recommend a
     * readahead, till it again confirms a sequential pattern.
     */
    next_read = 2*_GiB;
    ras.on_application_read(next_read, 1*_MiB);
    assert(ras.get_next_ra(4*_MiB) == 0);

    // 2nd read in the new section.
    next_read += 1*_MiB;
    ras.on_application_read(next_read, 1*_MiB);
    assert(ras.get_next_ra(4*_MiB) == 0);

    // 3rd read in the new section.
    next_read += 1*_MiB;
    ras.on_application_read(next_read, 1*_MiB);

    next_ra = next_read + 1*_MiB;
    assert(ras.get_next_ra(4*_MiB) == next_ra);

    /*
     * Read from the next section. We will only do random reads so pattern
     * detector should not see a seq pattern and must not recommend readahead.
     */
    next_read = 4*_GiB;
    ras.on_application_read(next_read, 1*_MiB);
    assert(ras.get_next_ra(4*_MiB) == 0);

    for (int i = 0; i < 1000; i++) {
        next_read = random_number(0, 1*_TiB);
        ras.on_application_read(next_read, 1*_MiB);
        assert(ras.get_next_ra(4*_MiB) == 0);

        next_read = random_number(1*_TiB, 2*_TiB);
        ras.on_application_read(next_read, 1*_MiB);
        assert(ras.get_next_ra(4*_MiB) == 0);
    }

    /*
     * Jump to a new section.
     * Here we will only do sequential reads. After 3 sequential reads, we
     * should detect the pattern and after that we should recommend readahead.
     */
    next_read = 10*_GiB;
    ras.on_application_read(next_read, 1*_MiB);
    assert(ras.get_next_ra(4*_MiB) == 0);

    next_read += 1*_MiB;
    ras.on_application_read(next_read, 1*_MiB);
    assert(ras.get_next_ra(4*_MiB) == 0);

    next_read += 1*_MiB;
    ras.on_application_read(next_read, 1*_MiB);

    next_ra = next_read+1*_MiB;
    assert(ras.get_next_ra(4*_MiB) == next_ra);

    for (int i = 0; i < 2000; i++) {
        next_read += 1*_MiB;
        ras.on_application_read(next_read, 1*_MiB);

        next_ra += 4*_MiB;
        assert(ras.get_next_ra(4*_MiB) == next_ra);

        complete_ra = next_ra;
        ras.on_readahead_complete(complete_ra, 4*_MiB);
    }

    // Stress run.
    for (int i = 0; i < 10'000'000; i++) {
        next_read = random_number(0, 1*_TiB);
        ras.on_application_read(next_read, 1*_MiB);
        assert(!ras.is_sequential());
        assert(ras.get_next_ra(4*_MiB) == 0);

        next_read = random_number(1*_TiB, 2*_TiB);
        ras.on_application_read(next_read, 1*_MiB);
        assert(!ras.is_sequential());
        assert(ras.get_next_ra(4*_MiB) == 0);

        next_read = random_number(2*_TiB, 3*_TiB);
        ras.on_application_read(next_read, 1*_MiB);
        assert(!ras.is_sequential());
        assert(ras.get_next_ra(4*_MiB) == 0);

        next_read = random_number(3*_TiB, 4*_TiB);
        ras.on_application_read(next_read, 1*_MiB);
        assert(!ras.is_sequential());
        assert(ras.get_next_ra(4*_MiB) == 0);
    }

    AZLogInfo("Unit testing ra_state, done!");

    return 0;
}

#ifdef DEBUG_READAHEAD
static int _i = ra_state::unit_test();
#endif

}
