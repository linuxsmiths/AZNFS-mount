#ifndef __AZNFSC_FILE_CACHE_H__
#define __AZNFSC_FILE_CACHE_H__

#include <map>
#include <mutex>
#include <memory>
#include <vector>
#include <list>
#include <atomic>
#include <chrono>

#include <cstring>
#include <cstdint>
#include <cassert>
#include <unistd.h>

#include "aznfsc.h"
#include "rpc_stats.h"
#include "fcsm.h"

struct nfs_inode;
struct rpc_task;

/*
 * Reminder to audit use of asserts to ensure we don't depend on assert
 * for error handling.
 *
 * XXX : This is temporarily disabled to support Release builds, but we need
 *       to audit this before releasing.
 */
#if 0
#ifdef NDEBUG
#error "Need to audit use of asserts in file_cache"
#endif
#endif

/*
 * Uncomment this if you want to use the tailroom from last chunk for
 * new get() requests asking for data right after the last chunk.
 * f.e., let's say application made following sequence of get() requests:
 * 1. get(0, 131072) - due to 128K read request.
 * 2. Now it reads from the backend and backend returned eof after say
 *    10 bytes. Caller will call release(10, 131062) to trim the last
 *    chunk for correctly representing the file size. This will not free
 *    the membuf but just reduce the bytes_chunk's length.
 * 3. Now caller makes the call get(10, 131062).
 *
 * With UTILIZE_TAILROOM_FROM_LAST_MEMBUF defined, the bytes_chunk that
 * we return will still point to the existing membuf. Without the define
 * this new bytes_chunk will get its own membuf of 131062 bytes.
 *
 * Though it saves space but it complicates things wrt setting of uptodate
 * MB_Flag. Since the uptodate flag is a property of the membuf the 2nd
 * caller who gets [10, 1310272) should not treat it as uptodate as it's
 * not (only first 10 bytes are uptodate).
 * Since this will not happen much in practice, for now keep it disabled
 * so that we don't have to worry about this complexity.
 *
 * IMPORTANT: With this is_whole will be set to false for all bytes_chunks
 *            returned for this range. This makes the cache less effective
 *            in most situations and unusable in some situations.
 *            If we decide to enable this we need to also make changes to
 *            merge the two bytes_chunks into a single bytes_chunk covering
 *            the entire membuf as soon as all existing users drop their
 *            inuse count and lock.
 */
//#define UTILIZE_TAILROOM_FROM_LAST_MEMBUF


namespace aznfsc {

/*
 * This is the maximum chunk size we allow. This is like our page size, but
 * unlike the usual page cache where every page is fixed size, our chunk cache
 * may have chunks of different sizes, though for the perfect case where
 * applications are doing sequential reads/writes all/most chunks would have
 * the max size. Infact, we want large chunks to reduce maintenance overhead.
 * Currently fuse kernel driver never sends any read/write IO larger than 1MB,
 * so that will end up being the chunk size, but for background IOs that we
 * initiate (readahead IOs) we will use the max chunk size.
 * A chunk need not map 1:1 to an NFS READ/WRITE RPC, though typically we will
 * issue one NFS RPC for one chunk but we can have 1:m or m:1 mappings where
 * multiple chunks are populated by one NFS RPC and vice versa.
 *
 * See comment above membuf::flag.
 */
#define AZNFSC_MAX_CHUNK_SIZE (4ULL * 1024 * 1024)

#define AZNFSC_BAD_OFFSET (~0ull)

#define PAGE_SIZE (4096ULL)

/*
 * Cache tag used for logging.
 * When inode is present, use inode number else address of bytes_chunk_cache
 * expressed as a 64-bit integer.
 */
#define CACHE_TAG (inode ? inode->get_fuse_ino() : (uint64_t) this)

// Forward declaration.
class bytes_chunk_cache;

/*
 * Note on membuf flags Dirty/Flushing/CommitPending
 * =================================================
 * When application data is copied to a membuf it becomes Dirty.
 * Dirty membufs are flushed to the server either when dirty data exceeds some
 * threshold or when the file is closed. While membuf is being flushed Flushing
 * bit is set. Once the membuf is successfully flushed, Dirty and Flushing bits
 * are cleared. If the flush was done using Unstable Write then CommitPending
 * bit is set. Once the membuf is successfully committed to the server,
 * CommitPending bit is cleared.
 * Till any of these bits is set, the membuf contains data which may not yet be
 * saved on the server and hence releasing the membuf may cause data loss.
 */
namespace MB_Flag {
    enum : uint32_t
    {
       Uptodate           = (1 << 0), // Fit for reading.
       Locked             = (1 << 1), // Exclusive access for updating membuf
                                      // data.
       Truncated          = (1 << 2), // Entire byte range covered by membuf is
                                      // truncated (shrink) from the file. Note
                                      // that even Dirty and/or inuse membufs
                                      // can be truncated, as we don't care
                                      // for cached data belonging to the
                                      // truncated region of the file.
       Dirty              = (1 << 3), // Data in membuf is newer than the Blob.
       Flushing           = (1 << 4), // Data from dirty membuf is being synced
                                      // to Blob (as UNSTABLE or FILE_SYNC Write).
       CommitPending      = (1 << 5), // Data from dirty membuf was synced using
                                      // UNSTABLE Write but it has not yet been
                                      // committed to the server.
    };
}

/**
 * Memory buffer, used for caching chunks in memory.
 * For file-backed bytes_chunk_cache membufs are realized by mmap()ed memory,
 * while for non file-backed bytes_chunk_cache membufs are realized by memory
 * allocated on the heap.
 */
struct membuf
{
    /**
     * Three things define a membuf:
     * 1. What offset inside the file it's caching.
     * 2. Count of bytes it's caching.
     * 3. Memory buffer address where the above data is stored.
     * 4. Backing file fd (in case of file-backed caches).
     *
     * We also have the bytes_chunk_cache backlink. This is strictly for
     * updating various cache metrics as membuf flags are updated.
     */
    membuf(bytes_chunk_cache *_bcc,
           uint64_t _offset,
           uint64_t _length,
           int _backing_file_fd = -1);

    /**
     * membuf destructor, frees the memory used by membuf.
     * This frees heap allocated memory for non file-backed membufs, while
     * for file-backed membufs the mmap()ed memory is munmap()ed.
     * Since membuf is used as a shared_ptr, this will be called when the
     * last ref to the shared_ptr is dropped, typically when the bytes_chunk
     * referring to the membuf is discarded.
     */
    ~membuf();

    /*
     * bytes_chunk_cache to which this membuf belongs.
     * This is strictly for updating various cache metrics as membuf flags are
     * updated.
     * Those are atomic, hence it's ok to access w/o serializing access to
     * the cache.
     */
    bytes_chunk_cache *bcc = nullptr;

    /*
     * This membuf caches file data in the range [offset, offset+length).
     * A membuf starts caching [initial_offset, initial_offset+initial_length),
     * but can be later trimmed to cache a smaller section of the file,
     * [offset, offset+length) as some release() call releases part of the
     * chunk. For trimming from right, only length needs to be reduced, while
     * for trimming from left, length is reduced and offset is increased.
     * The corresponding chunkmap[]'s offset, length and buffer_offset are also
     * updated accordingly and they should always be same as that of the membuf.
     *
     * initial_offset and initial_length hold the initial values that membuf
     * was created with, these remain unchanged for the life of the membuf.
     */
    const uint64_t initial_offset;
    const uint64_t initial_length;
    std::atomic<uint64_t> offset;
    std::atomic<uint64_t> length;

    /*
     * Actual allocated length. This can be greater than length for
     * file-backed membufs. See comments above allocated_buffer.
     * Once set this will not change, even when the membuf is drop'ed and
     * allocated_buffer becomes nullptr.
     */
    uint64_t allocated_length = 0;

    // Backing file fd (-1 for non file-backed caches).
    const int backing_file_fd = -1;

    /*
     * Data buffer used for cacheing. This will be mmap()ed memory for
     * file-backed caches and heap buffer for non file-backed ones. For non
     * file-backed caches, these will never be nullptr, but for file-backed
     * caches, nullptr means that the cache is dropped and we need to load the
     * data from the backing file.
     *
     * Since mmap() can only be done for page aligned file offsets, we need
     * allocated_buffer to track the page aligned mmap()ed address, while
     * buffer is the actual buffer address to use for storing cached data.
     * For non file-backed membufs, both will be same.
     * This also means that for file-backed caches the actual allocated bytes
     * is "length + (buffer - allocated_buffer)". See allocated_length.
     *
     * Once set buffer and allocated_buffer should not change. For file-backed
     * caches drop() will munmap() and set allocated_buffer to nullptr.
     *
     * TODO: trim() doesn't update buffer, which means membuf::buffer does not
     *       point to the correct data cached by this membuf. Always use
     *       bytes_chunk::get_buffer() to get the correct address.
     */
    uint8_t *buffer = nullptr;
    uint8_t *allocated_buffer = nullptr;

    /*
     * If is_file_backed() is true then 'allocated_buffer' is the mmap()ed
     * address o/w it's the heap allocation address.
     */
    bool is_file_backed() const
    {
        return (backing_file_fd != -1);
    }

    // Returns buffer address for storing the data.
    uint8_t *get() const
    {
        return buffer;
    }

    /**
     * Drop data cached in memory.
     * This is a no-op for non file-backed membufs since for them memory
     * is the only place where data is stored. For file-backed membufs this
     * drops data from the memory while the data is still present in the file.
     * load() can be used to reload data in memory cache.
     *
     * Returns the number of bytes reclaimed by dropping the cache. A -ve
     * return indicates error in munmap().
     */
    int64_t drop();

    /**
     * Load data from file backend into memory.
     * This is a no-op for non file-backed membufs since for them the data
     * is always in memory.
     * load() assumes that the backing file is present and has a size at least
     * equal to offset+length, so that it can map valid data. The backing file
     * obviously will need to be invalidated if the file's data has changed
     * (conveyed by mtime/size change) and anytime the backing file is
     * invalidated all membufs referring to data inside the file MUST be
     * destroyed first.
     */
    bool load();

    uint32_t get_flag() const
    {
        return flag;
    }

    /**
     * Is membuf uptodate?
     * Only uptodate membufs are fit for reading.
     * A newly created membuf is not uptodate and must be set uptodate
     * after reading the required data from the Blob.
     */
    bool is_uptodate() const
    {
        return (flag & MB_Flag::Uptodate);
    }

    void set_uptodate();
    void clear_uptodate();

    /*
     * wait-for-uptodate is a two step operation, the pre_unlock must be called
     * with the membuf locked while the post_unlock must be called after
     * releasing the membuf lock. post_unlock is the one that does the actual
     * waiting, if needed.
     */
    void wait_uptodate_pre_unlock();
    void wait_uptodate_post_unlock();

    bool is_locked() const
    {
        const bool locked = (flag & MB_Flag::Locked);
        /*
         * XXX Following assert is usually true but for the rare case when
         *     caller may drop the inuse count while holding the lock for
         *     release()ing the chunk, this will not hold.
         *     See read_callback() for such usage.
         */
#if 0
        // If locked, must be inuse.
        assert(is_inuse() || !locked);
#endif
        return locked;
    }

    /**
     * set_locked() returns true if it got the lock w/o having to wait.
     */
    bool set_locked();
    void clear_locked();
    bool try_lock();

    /**
     * A membuf is marked dirty when the membuf data is updated, making it
     * out of sync with the Blob contents for the range. This should be done
     * by writer threads which write application data into membuf. A dirty
     * membuf must be written to the Blob before it can be freed. Once written,
     * it should be marked not-dirty by calling clear_dirty().
     */
    bool is_dirty() const
    {
        const bool dirty = (flag & MB_Flag::Dirty);

        /*
         * Make sure is_dirty returns true only when is_uptodate() is true
         * otherwise we may write garbage data to Blob.
         * If membuf is dirty, it must not be in commit pending state.
         * A membuf goes to commit pending state only after it's successfully
         * flushed to the server (using UNSTABLE writes), and then it's no
         * longer dirty, and new application data cannot be copied over it
         * till it's committed.
         */
        assert(!dirty || is_uptodate());
        assert(!dirty || !(flag & MB_Flag::CommitPending));

        return dirty;
    }

    void set_dirty();
    void clear_dirty();

    /**
     * Is this membuf truncated as a result of file truncate/shrink?
     * Truncated membufs are immediately removed from the cache, so this
     * is mainly for asserting in ~membuf().
     */
    bool is_truncated() const
    {
        return (flag & MB_Flag::Truncated);
    }
    void set_truncated();

    bool is_flushing() const
    {
        const bool flushing = (flag & MB_Flag::Flushing);

        /*
         * If membuf is in flushing state, it means it can't be in
         * commit pending state. Only after successfully flushing
         * (using UNSTABLE writes) it goes to commit pending state.
         */
        assert(!flushing || !(flag & MB_Flag::CommitPending));

        /*
         * If flushing, must be dirty, but we cannot safely assert here
         * as those flags may be cleared while we are accessing them.
         */
#if 0
        assert(!flushing || (flag & MB_Flag::Dirty));
#endif

        return flushing;
    }

    void set_flushing();
    void clear_flushing();

    bool is_commit_pending() const
    {
        const bool commit_pending = (flag & MB_Flag::CommitPending);

        /*
         * membuf can be marked CommitPending only after it's successfully
         * written to the server as unstable write. Till it's committed we
         * cannot copy new data to the membuf else we risk overwriting data
         * which is not yet committed to the server.
         */
        assert(!commit_pending || !(flag & MB_Flag::Dirty));
        assert(!commit_pending || !(flag & MB_Flag::Flushing));

        return commit_pending;
    }

    void set_commit_pending();
    void clear_commit_pending();

    bool is_inuse() const
    {
        return (inuse > 0);
    }

    int get_inuse() const
    {
        return inuse;
    }

    void set_inuse();
    void clear_inuse();

    /**
     * trim 'trim_len' bytes from the membuf. 'left' should be true if trimming
     * is done from the left side of the membuf, else trimming is done from the
     * right side of the membuf.
     * It'll perform necessary validations and updations needed when a membuf is
     * trimmed.
     * Caller must make sure that this membuf is not being accessed by some other
     * thread. and trim() can safely update it. This means, trim() can be called
     * in one of the following cases:
     * 1. membuf is locked.
     *    See bytes_chunk_cache::truncate().
     * 2. chunkmap lock is held and membuf is not inuse.
     *    See bytes_chunk_cache::scan()->safe_to_release().
     *
     * Called from release() and truncate(), release() can trim both from left
     * and right while truncate() can only trim from right.
     */
    void trim(uint64_t trim_len, bool left);

private:
    /*
     * Lock to correctly read and update the membuf state.
     *
     * Note on safely accessing membuf
     * ===============================
     * Since multiple threads may be trying to read and write to the same file
     * or part of the file, we need to define some rules for ensuring consistent
     * access. Here are the rules:
     *
     * 1. Any reader or writer gets access to membuf by a call to
     *    bytes_chunk_cache::get(). membufs are managed by shared_ptr, hence
     *    the reader/writer is guaranteed that as long as it does not destroy
     *    the returned bytes_chunk, the underlying membuf will not be freed.
     *    Note that consistent read/write access though needs synchronization
     *    among the various reader/writer threads, read on.
     * 2. A thread trying to write to the membuf must get exclusive access to
     *    the membuf. It can get that by calling set_locked(). set_locked()
     *    will block if the lock is already held by some other thread and will
     *    return after acquiring the lock. Blocking threads will wait on the
     *    condition_variable 'cv' and will be woken up when the current locking
     *    thread unlocks. Note that membuf will be written under the following
     *    cases:
     *     i) A writer writes user passed data to the membuf.
     *    ii) A reader reads data from the Blob into the membuf.
     * 3. Membufs also have an inuse count which indicates if there could be
     *    an ongoing IO (whether there is actually an ongoing IO can be
     *    found by using the locked bit). The purpose of inuse count is to
     *    just mark the membuf such that clear() doesn't clear membufs which
     *    might soon afterwards have IOs issued.
     *    bytes_chunk_cache::get() will bump the inuse count of all membufs
     *    it returns since the caller most likely might perform IO on the
     *    membuf. It's caller's responsibility to clear the inuse by calling
     *    clear_inuse() once they are done performing the IO. This should be
     *    done after performing the IO, and releasing the lock taken for the
     *    IO.
     * 4. A newly created membuf does not have valid data and hence a reader
     *    should not read from it. Such an membuf is "not uptodate" and a
     *    reader must first read the corresponding file data into the membuf,
     *    and mark the membuf "uptodate" after successfully reading the data
     *    into it. It can do that after getting exclusive access to membuf
     *    by calling set_locked(). Any other reader which accesses the membuf
     *    in the meantime will find it "not update" and it'll try to update
     *    the membuf itself but it'll find the membuf locked, so set_locked()
     *    will cause the thread to wait on 'cv'. Once the current reader
     *    updates the membuf, it marks it "uptodate" by calling set_uptodate()
     *    and then unlock it by calling clear_locked(). Other readers waiting
     *    for the lock will get woken up and they will discover that the
     *    membuf is uptodate by checking is_uptodate() and they can then read
     *    that data into their application data buffers.
     *
     *    IMPORTANT RULES FOR UPDATING THE "UPTODATE" BIT
     *    ===============================================
     *    - Any reader that gets a bytes_chunk whose membuf is not uptodate,
     *      must try to read data from the Blob, but only mark it uptodate if
     *      is_whole was also true for the bytes_chunk. This is because
     *      is_whole will be true for bytes_chunk representing "full membuf"
     *      (see UTILIZE_TAILROOM_FROM_LAST_MEMBUF) and hence they only can
     *      correctly mark the membuf as uptodate. Other readers, if they get
     *      the lock first, they can issue the Blob read to update the part
     *      of membuf referred by their bytes_chunk, and return that data to
     *      fuse, but they cannot mark the membuf as uptodate, so future users
     *      cannot benefit from their read.
     *      So, ONLY IF maps_full_membuf() returns true for a bytes_chunk, the
     *      reader MUST mark the membuf uptodate.
     *
     *    - Writers must set the uptodate bit only if they write the entire
     *      membuf (maps_full_membuf() returns true), else they should not
     *      change the uptodate bit.
     *
     * 5. If a reader finds that a membuf is uptodate (as per is_uptodate()), it
     *    can return the membuf data to the application. Note that some writer
     *    may be writing to the data simultaneously and reader may get a mix
     *    of old and new data. This is fine as per POSIX. Users who care about
     *    this must synchronize access to the file.
     * 6. Once an membuf is marked uptodate it remains uptodate for the life
     *    of the membuf, unless one of the following happens:
     *     i) We detect via file mtime change that our cached copy is no longer
     *        valid. In this case the entire cache for that file is clear()ed
     *        which causes all bytes_chunk and hence all membufs to be freed.
     *    ii) An NFS read from the given portion of Blob fails. We will need to
     *        understand the effects of this better, since we normally never
     *        fail an NFS IO (think hard mount).
     * 7. A writer MUST observe the following rules:
     *     i) If writer is writing to a part of the membuf, it MUST ensure
     *        that membuf is uptodate before it can modify part of the membuf.
     *        It must do that by reading the *entire* membuf from the Blob,
     *        and marking the membuf as uptodate. Then it must update the part
     *        it wants to. After writing it must mark the membuf as dirty.
     *        All of this must be done while holding the lock on the membuf as
     *        we want the membuf update to be atomic.
     *    ii) If writer is writing to the entire membuf (maps_full_membuf()
     *        returns true), it can directly write to the membuf even if it's
     *        not already uptodate, and after writing it must mark the membuf
     *        as uptodate and dirty.
     * 7. A writer must mark the membuf dirty by calling set_dirty(), after it
     *    updates the membuf data. Dirty membufs must be synced with the Blob
     *    at some later time and once those writes to Blob succeed, the membuf
     *    dirty flag must be cleared by calling clear_dirty(). Note that a
     *    dirty membuf is still uptodate since it has the latest content for
     *    the reader.
     */
    std::mutex mb_lock_44;

    /*
     * Flag bitmap for correctly defining the state of this membuf.
     * This is a bitwise or of zero or more MB_Flag values.
     *
     * Note: membuf::flag represents the state of the entire membuf,
     *       irrespective of the offset within the membuf a particular
     *       bytes_chunk represents. This means even if one thread has to
     *       read say 1 byte but the actual bytes_chunk created by another
     *       thread is of size 1GB, the former thread has to wait till the
     *       entire 1GB data is read by the other thread and the membuf is
     *       marked MB_Flag::Uptodate.
     *       This means very large membufs will cause unnecessary waits.
     *       Test out and find a good value for AZNFSC_MAX_CHUNK_SIZE.
     */
    std::atomic<uint32_t> flag = 0;

    // For managing threads waiting on MB_Flag::Locked.
    std::condition_variable cv;

    /*
     * Incremented by bytes_chunk_cache::get() before returning a membuf to
     * the caller. Caller must decrement it once they are done reading or
     * writing the membuf.
     */
    std::atomic<uint32_t> inuse = 0;
};

/**
 * This represents one contiguous chunk of bytes in bytes_chunk_cache.
 * bytes_chunk_cache consists of zero or more bytes_chunk ordered by offset.
 * Note that a byte range can be cached using one or more bytes_chunk and the
 * size of the individual component bytes_chunk depends on the order in which
 * the application writes data to the file.
 * A contiguous file range cached by a series of bytes_chunk is called an
 * "extent". Extents are important as they decide if/when we can issue full
 * block-sized write to the Blob.
 */
struct bytes_chunk
{
    // bytes_chunk_cache needs to access the private member alloc_buffer.
    friend bytes_chunk_cache;

private:
    // bytes_chunk_cache to which this chunk belongs.
    bytes_chunk_cache *bcc = nullptr;

    /*
     * This is the underlying membuf. The actual buffer where data is stored
     * can be found by adding buffer_offset to this, and can be retrieved using
     * the convenience function get_buffer(). buffer_offset is typically 0 but
     * it can be non-zero when multiple chunks are referring to the same buffer
     * but at different offsets (e.g., cache trimming).
     * Any chunk that refers to the same allocated buffer will hold a ref to
     * alloc_buffer, so alloc_buffer will be freed when the last ref is dropped.
     * This should typically happen when the chunk is freed.
     *
     * To find the length of the allocated buffer, use alloc_buffer->length.
     */
    std::shared_ptr<membuf> alloc_buffer;

public:
    /*
     * Offset from the start of file this chunk represents.
     * For bytes_chunks stored in chunkmap[] this will be incremented to trim
     * a bytes_chunk from the left and must always match membuf::offset while
     * for non-chunkmap bcs (those returned by get()/getx()) this will be the
     * copy of membuf::offset at the time the bc was created, membuf can be
     * later trimmed and its offset may increase.
     */
    uint64_t offset = 0;

    /*
     * Length of this chunk.
     * User can safely access [get_buffer(), get_buffer()+length).
     * For bytes_chunks stored in chunkmap[] this will be reduced to trim
     * a bytes_chunk from left and right and must always match membuf::length
     * while for non-chunkmap bcs (those returned by get()/getx()) this will be
     * the copy of membuf::length at the time the bc was created, membuf can be
     * later trimmed and its length may reduce.
     */
    uint64_t length = 0;

    /*
     * Offset of buffer from alloc_buffer->get().
     * For bytes_chunks stored in chunkmap[] this will be incremented to trim
     * a bytes_chunk from the left.
     */
    uint64_t buffer_offset = 0;

    /*
     * Private data. User can use this to store anything they want. but
     * most commonly it's used to update the progress as the bc can be read
     * or written in parts. Hence "bc.offset + bc.pvt" is the next offset
     * to read/write and "bc.length - bc.pvt" is the remaining length to
     * read/write and "bc.get_buffer() + bc.pvt" is the address where the
     * data must be read/written.
     *
     * This is opaque to the cache and cache doesn't use it. Hence for bcs
     * stored in the chunkmap, pvt will be 0.
     *
     * TODO: Shall we designate pvt for this specific job and rename this to
     *       something more specific like cursor.
     */
    uint64_t pvt = 0;

    /*
     * Number of backend calls issued to sync this byte chunk with the backing
     * blob. It could be read call(s) to read data from the blob or it could be
     * write call(s) to sync dirty byte chunk.
     *
     * Note: Values greater than 1 signify partial read/write calls.
     */
    int num_backend_calls_issued = 0;

    /*
     * bytes_chunk is a window/view into the underlying membuf which holds the
     * actual data. It can refer to the entire membuf or any contiguous part
     * of it. This tells if a bytes_chunk refers to the full membuf or a part.
     * This is useful for the caller to know as based on this they can decide
     * how the membuf flags need to be updated when peforming IOs through this
     * bytes_chunk. f.e., if a reader has a bytes_chunk refering to a partial
     * membuf and they perform a successful read, they cannot mark the membuf
     * uptodate as they have not read data for the entire membuf.
     * OTOH if a write has a bytes_chunk refering to a partial membuf and they
     * write data into the bytes_chunk they MUST mark the membuf dirty as
     * updating even a single byte makes the membuf dirty. At the same time if
     * the membuf is not uptodate, the writer cannot simply copy into the
     * bytes_chunk, as only uptodate membufs can be partially updated.
     *
     * Note: Note that trimming doesn't make changes to the membuf but instead
     *       it changes the corresponding bytes_chunk in the chunkmap[], so
     *       is_whole indicates whether a bytes_chunk returned by get()
     *       refers to the complete bytes_chunk in the chunkmap[] or part of it.
     *
     * Note: This is set at the time when the bytes_chunk is created (and
     *       returned by get()/getx()/scan()), depending on whether the
     *       returned bytes_chunk fully covers the corresponding chunkmap
     *       bytes_chunk or it refers to only partial chunkmap bytes_chunk.
     *       If chunkmap bytes_chunk changes afterwards, this may not be
     *       correct. Note that an inuse chunkmap bytes_chunk can (only) be
     *       updated by a truncate() call.
     */
    bool is_whole = false;

    /*
     * is_new indicates whether a bytes_chunk returned by a call to
     * bytes_chunk_cache::get() refers to a freshly allocated membuf or it
     * refers to an existing membuf. is_new implies that membuf doesn't contain
     * valid data and hence the uptodate membuf flag must be false. It only
     * makes sense for bytes_chunk returned by bytes_chunk_cache::get(), and not
     * for bytes_chunk which are stored in bytes_chunk_cache::chunkmap.
     * Since membufs are allocated to fit caller's size req, bytes_chunk with
     * is_new set MUST have is_whole also set.
     */
    bool is_new = false;

    /**
     * Get the inode corresponding to this bc.
     */
    struct nfs_inode *get_inode() const;

    /**
     * Return membuf corresponding to this bytes_chunk.
     * This will be used by caller to synchronize operations on the membuf.
     * See membuf::flag and various operations that can be done on them.
     */
    struct membuf *get_membuf() const
    {
        struct membuf *mb = alloc_buffer.get();

        // membuf must have valid alloc_buffer at all times.
        assert(mb != nullptr);

        return mb;
    }

    /**
     * Returns usecount for the underlying membuf.
     * A bytes_chunk added only to bytes_chunk_cache::chunkmap has a usecount
     * of 1 and every user that calls get() will get one usecount on the
     * respective membuf.
     */
    int get_membuf_usecount() const
    {
        return alloc_buffer.use_count();
    }

    /**
     * Start of valid cached data corresponding to this chunk.
     * This will typically have the value alloc_buffer->get(), i.e., it points
     * to the start of the data buffer represented by the shared pointer
     * alloc_buffer, but if some cached data is deleted from the beginning of a
     * chunk, causing the buffer to be "trimmed" from the beginning, this can
     * point anywhere inside the buffer.
     */
    uint8_t *get_buffer() const
    {
        // Should not call on a dropped cache.
        assert(alloc_buffer->get() != nullptr);

        /*
         * buffer_offset should point to a valid membuf byte.
         * Note that this bytes_chunk is a snapshot of the chunkmap's
         * bytes_chunk at some point in the past (and it not necessarily the
         * chunkmap bc, which should always be in sync with membuf), so it's
         * possible that after this bc was returned by a get()/getx() call,
         * user may have release()d the chunk which would cause membuf to be
         * trimmed, so we cannot safely compare with the current membuf length
         * and offset fields, but one thing we can say for sure is that
         * buffer_offset should never exceed the membuf's initial_length.
         */
        assert(buffer_offset < alloc_buffer->initial_length);

        return alloc_buffer->get() + buffer_offset;
    }

    /**
     * Does this bytes_chunk cover the "full membuf"?
     * Note that "full membuf" refers to membuf after trimming if any.
     */
    bool maps_full_membuf() const
    {
        assert(!is_new || is_whole);
        return is_whole;
    }

    /**
     * Is it safe to release (remove from chunkmap) this bytes_chunk?
     * bytes_chunk whose underlying membuf is either inuse or dirty or commit
     * pending are not safe to release because:
     * - dirty indicates membuf has some data which needs to be flushed, so
     *   we cannot release it w/o flushing the data.
     * - inuse indicates some other thread is doing something with the membuf,
     *   maybe it's writing fresh data to it and may mark it dirty. If we
     *   allow such membuf to be released, future readers who get() the cache
     *   will miss those changes.
     * - commit pending means those membufs are sitting in the TBL of the Blob,
     *   not yet committed, server may fail the commit in which case we might
     *   have to resend those to the server and hence we cannot free those
     *   till successful commit.
     *
     * Note: Since we do not allow inuse membufs to be released, it means
     *       if caller is owning the membuf they must drop their inuse count
     *       before calling release().
     */
    bool safe_to_release() const
    {
        const struct membuf *mb = get_membuf();
        return !mb->is_inuse() && !mb->is_dirty() && !mb->is_commit_pending();
    }

    /**
     * Does this bytes_chunk need to be flushed?
     * bytes_chunk whose underlying membuf is dirty and not already being
     * flushed, qualify for flushing.
     */
    bool needs_flush() const
    {
        const struct membuf *mb = get_membuf();
        return mb->is_dirty() && !mb->is_flushing();
    }

    /**
     * Constructor to create a brand new chunk with newly allocated buffer.
     * This chunk is the sole owner of alloc_buffer and 'buffer_offset' is 0.
     * Later as this chunk is split or returned to the caller through get(),
     * alloc_buffer may have more owners. When the last owner releases claim
     * alloc_buffer will be freed. This should happen when the chunk is freed.
     *
     * XXX: If we need to gracefully handle allocation failure, the buffer
     *      allocation must be done by the caller.
     *
     * XXX Default std new[] implementation is very slow, use tcmalloc for
     *     much faster perf. The main problem with std new is that it doesn't
     *     use memory pools and for large allocations it gets/releases memory
     *     to the system, which causes zero'ing overhead as kernel has to
     *     zero pages.
     */
    bytes_chunk(bytes_chunk_cache *_bcc,
                uint64_t _offset,
                uint64_t _length);

    /**
     * Constructor to create a chunk that refers to alloc_buffer from another
     * existing chunk. The additional _buffer_offset allows flexibility to
     * each chunk to point anywhere inside alloc_buffer.
     * This is useful for chunks created due to splitting or when returning
     * bytes_chunk from bytes_chunk_cache::get().
     */
    bytes_chunk(bytes_chunk_cache *_bcc,
                uint64_t _offset,
                uint64_t _length,
                uint64_t _buffer_offset,
                const std::shared_ptr<membuf>& _alloc_buffer,
                bool _is_whole = true,
                bool _is_new = false);

    /**
     * Copy constructor, only for use by test code.
     */
    bytes_chunk(const bytes_chunk& rhs) :
        bytes_chunk(rhs.bcc,
                    rhs.offset,
                    rhs.length,
                    rhs.buffer_offset,
                    rhs.alloc_buffer,
                    rhs.is_whole,
                    rhs.is_new)

    {
        // new bytes_chunk MUST cover whole membuf.
        assert(!is_new || is_whole);

        pvt = rhs.pvt;
        num_backend_calls_issued = rhs.num_backend_calls_issued;
    }

    /**
     * Default constructor and assignment operator, only for use by test code.
     */
    bytes_chunk() = default;
    bytes_chunk& operator=(const bytes_chunk&) = default;


#ifdef UTILIZE_TAILROOM_FROM_LAST_MEMBUF
    /**
     * Return available space at the end of buffer.
     * This is usually helpful when a prev read() was short and could not fill
     * the entire buffer and then a subsequent read() is issued to fill
     * subsequent data.
     */
    uint64_t tailroom() const
    {
        const int64_t tailroom =
            (alloc_buffer->length - (buffer_offset + length));
        assert(tailroom >= 0);
        assert(tailroom <= (int64_t) AZNFSC_MAX_CHUNK_SIZE);

        return tailroom;
    }
#endif

    /**
     * Drop data cached in memory, for this bytes_chunk.
     *
     * Returns the number of bytes reclaimed by dropping the cache. A -ve
     * return indicates error in munmap().
     */
    int64_t drop()
    {
        assert(get_membuf_usecount() > 0);

        /*
         * If the membuf is being used by someone else, we cannot drop/munmap
         * it, o/w users accessing the data will start getting errors.
         */
        if (get_membuf_usecount() == 1) {
            return alloc_buffer->drop();
        }

        return 0;
    }

    /**
     * Load data from file backend into memory, for this bytes_chunk.
     */
    void load()
    {
        [[maybe_unused]] const bool ret = alloc_buffer->load();
        assert(ret);
    }
};

/**
 * bytes_chunk_cache::scan() can behave differently depending on the scan_action
 * passed.
 */
enum class scan_action
{
    SCAN_ACTION_INVALID = 0,
    SCAN_ACTION_GET,
    SCAN_ACTION_RELEASE,
};

/**
 * This is the per-file cache that caches variable sized extents and is
 * indexed using byte offset and length.
 *
 * Note on read/write performance using bytes_chunk_cache
 * ======================================================
 * If you use file-backed bytes_chunk_cache then the performance of that
 * will be limited by the backing file read/write performance as the data
 * read from the NFS server is placed into the read buffers which are actually
 * the mmap()ed buffers, hence the steady state write performance will be
 * limited by the file write throughput. Having said that, if you have large
 * amount of RAM and the file being read can fit completely in RAM, then the
 * read will happen very fast and then the data can be flushed to the backing
 * file later.
 * OTOH, if you use non file-backed cache, and make sure you release the
 * chunks as they are read from the server, then the read performance is only
 * limited by the memory write speed.
 * Similar log applies to write.
 */
class bytes_chunk_cache
{
    friend membuf;
    friend bytes_chunk;

public:
    bytes_chunk_cache(struct nfs_inode *_inode,
                      const char *_backing_file_name = nullptr);

    ~bytes_chunk_cache();

    /**
     * Call this to check if the cache is empty, i.e., newly allocated.
     */
    bool is_empty() const
    {
        /*
         * TSAN Warning.
         * FIXME:
         * If we call it while bytes_chunk_cache::scan() is adding to chunkmap,
         * TSAN complains of data race.
         * We need to fix this, though usually the caller is not strictly
         * depending on the result returned by this, as the cache can change
         * right after the call.
         */
        return chunkmap.empty();
    }

    /**
     * Return a vector of bytes_chunk that cache the byte range
     * [offset, offset+length). Parts of the range that correspond to chunks
     * already present in the cache will refer to those existing chunks, for
     * such chunks is_new will be set to false, while those parts of the
     * range for which there wasn't an already cached chunk found, new chunks
     * will be allocated and inserted into the chunkmap. These new chunks will
     * have is_new set to true. This means after this function successfully
     * returns there will be chunks present in the cache for the entire range
     * [offset, offset+length).
     *
     * This can be called by both,
     * - writers, who want to write to the specified range in the file.
     *   The returned chunks is a scatter list where the caller should write.
     *   bytes_chunk::buffer is the buffer corresponding to each chunk where
     *   caller should write bytes_chunk::length amount of data.
     * - readers, who want to read the specified range from the file.
     *   The returned chunks is a scatter list containing the data from the
     *   file.
     *
     * TODO: Reuse buffer from prev/adjacent chunk if it has space. Currently
     *       we will allocate a new buffer, this works but is wasteful.
     *       e.g.,
     *       get(0, 4096)
     *       release(10, 4086) <-- this will just update length but the buffer
     *                             will remain.
     *       get(10, 4086)  <-- this get() should reuse the existing buffer.
     *
     *       Update: This is now done, but we still haven't generalized the
     *               solution to reuse buffer for all cases, but the most
     *               common case is now addressed! Leaving the TODO for
     *               tracking the generalized case.
     *       Update2: This introduces challenges, so it's turned off for now.
     *                see UTILIZE_TAILROOM_FROM_LAST_MEMBUF.
     *
     * Note: Caller must do the following for correctly using the returned
     *       bytes_chunks:
     *
     *       1. Since get() increments the inuse count for each membuf it
     *          returns, caller must call clear_inuse() once it's done
     *          performing IO on the membuf. For writers it'll be after they
     *          are done copying application data to the membuf and marking
     *          it dirty, and for readers it'll be after they are done reading
     *          data from the Blob into the membuf (if it's not already
     *          uptodate). Once the caller drops inuse count
     *          bytes_chunk_cache::clear() can potentially remove the membuf
     *          from the cache, so the caller must make sure that it drops
     *          inuse count only after correctly setting the state,
     *          i.e., call set_dirty() after writing to the membuf.
     *       2. IOs can be performed to the membuf only after locking it using
     *          set_locked(). Once the IO completes release the lock using
     *          clear_locked(). This must be done before calling clear_inuse().
     *          So the logical seq of operations are:
     *          >> get()
     *          >> for each bytes_chunk returned
     *          >>      set_locked()
     *          >>      perform IO
     *          >>      clear_locked()
     *          >>      clear_inuse()
     *
     * Note: Usually inuse count should be dropped after the lock is released,
     *       but there's one case where you may drop inuse count while the lock
     *       is held. This is if you want to call bytes_chunk_cache::release()
     *       but want the lock for setting the membuf uptodate f.e.
     *       See read_callback() for such usage.
     */
    std::vector<bytes_chunk> get(uint64_t offset, uint64_t length)
    {
        num_get++;
        num_get_g++;
        bytes_get += length;
        bytes_get_g += length;

        /*
         * Perform inline pruning if needed.
         * We do inline pruning when we are "extremely" high on memory usage
         * and hence cannot proceed w/o making space for the new request.
         */
        inline_prune();

        return scan(offset, length, scan_action::SCAN_ACTION_GET,
                    nullptr /* bytes_released */,
                    nullptr /* extent_left */,
                    nullptr /* extent_right */);
    }

    /**
     * Same as get() but to be used by writers who want to write to the given
     * cache range but are also interested in knowing when enough dirty data is
     * accumulated that they may want to flush/sync. Caller can pass two
     * uint64_t pointers to find out the largest contiguous dirty byte range
     * containing the requested byte range. Based on their wsize setting or some
     * other criteria, caller can then decide if they want to flush the dirty
     * data.
     * If extent_left and extent_right are non-null, on completion they will
     * hold the left and right edges of the extent containing the range
     * [offset, offset+length). Note that an extent is a collection of one or
     * more membufs which cache contiguous bytes.
     *
     * Note: [extent_left, extent_right) range contains one or more *full*
     *       membufs, also those membufs are dirty and not already flushing.
     *       If [offset, offset+length) falls on existing membuf(s) then we
     *       include those in [extent_left, extent_right) irrespective of the
     *       dirty/flushing flags since part of the membuf(s) is going to be
     *       updated and membuf will become dirty and hence would need to be
     *       flushed. BUT, as usual the caller must check the uptodate flag to
     *       decide if it needs to do a Read-Modify-Write before flushing.
     *
     * Note: Once the getx() call returns the extent details, since it doesn't
     *       hold membuf lock on any of the membufs in the extent range, some
     *       other thread can potentially initiate sync/flush of the membuf(s).
     *       Though this should not be common since the thread writing data
     *       is the first one to know about it, but depending on whether you
     *       have parallel writers or some periodic flusher thread, it can
     *       happen.
     */
    std::vector<bytes_chunk> getx(uint64_t offset,
                                  uint64_t length,
                                  uint64_t *extent_left,
                                  uint64_t *extent_right)
    {
        num_get++;
        num_get_g++;
        bytes_get += length;
        bytes_get_g += length;

        /*
         * Perform inline pruning if needed.
         * We do inline pruning when we are "extremely" high on memory usage
         * and hence cannot proceed w/o making space for the new request.
         */
        inline_prune();

        return scan(offset, length, scan_action::SCAN_ACTION_GET,
                    nullptr /* bytes_released */, extent_left, extent_right);
    }

    /**
     * Try and release chunks in the range [offset, offset+length) from
     * chunkmap. Only chunks which are fully contained inside the range would
     * be released, while chunks which lie partially in the range are trimmed
     * (by updating the buffer, length and offset members). These will be
     * released later when a future release() call causes them to contain no
     * valid data. release() will skip/ignore any byte range that's not
     * currently cached.
     *
     * Note that a release() call is an advice and not an order. Only those
     * chunks will be released which are not actively being used. Following
     * chunks won't be released:
     * - Which are inuse.
     *   These may have ongoing IOs, so not safe to release.
     * - Which are dirty.
     *   These need to be flushed to the Blob, else we lose data.
     *
     * Additionally, release() will *not* trim chunks unless the release()d
     * range aligns with either the left or the right edge, i.e., for ranges
     * falling in the middle of a chunk will be skipped.
     *
     * If release() successfully releases one or more chunks, a subsequent
     * call to get() won't find them in the chunkmap and hence will allocate
     * fresh chunk (with is_new true).
     *
     * Note that release() removes the chunks from chunkmap and drops the
     * original ref on the membufs. The membuf itself won't be freed till the
     * last ref on it is dropped, i.e., users can safely access membuf(s)
     * returned by get() even if some other thread calls release().
     *
     * It returns the number of bytes actually released. These could be full
     * chunks or partial chunks (both of which are not currently in use).
     * Caller can use this to decide if he wants to update the membuf flags
     * f.e., if a reader gets a bc of 100 bytes but when it read the backing
     * file it got eof after 10 bytes, it should try to release [10, 100)
     * byte range. If it's able to release successfully that would mean that
     * he is the sole owner and hence it can mark it uptodate, else it cannot
     * release and cannot mark uptodate.
     *
     * Note: For releasing all chunks and effectively nuking the cache, use
     *       clear(), but note that clear() also won't release above chunks,
     *       for which safe_to_release() returns false.
     */
    uint64_t release(uint64_t offset, uint64_t length)
    {
        uint64_t bytes_released;

        num_release++;
        num_release_g++;

        scan(offset, length, scan_action::SCAN_ACTION_RELEASE, &bytes_released);
        assert(bytes_released <= length);

        bytes_release += bytes_released;
        bytes_release_g += bytes_released;

        return bytes_released;
    }

    /**
     * Truncate cache to 'trunc_len' bytes.
     * Any cached byte(s) with offset >= 'trunc_len' will be removed from the
     * cache. All fully truncated bcs (having bc.offset >= 'trunc_len') will be
     * removed from chunkmap. Note that 'trunc_len' can fall inside a bc, such
     * partially truncated bc will be trimmed from the right to remove any bytes
     * after 'trunc_len'.
     * There are two ways to call truncate(), with 'post' as false or true.
     * If 'post' is false, it'll lock all the affected bcs, so it may need to wait
     * for ongoing IOs on those membufs to complete, hence it can take a long time
     * depending on how many bytes_chunk are affected by truncate and if there are
     * IOs ongoing on those. If 'post' is false then it calls membuf::try_lock()
     * and skips truncating the membufs which it could not lock.
     * Caller will typically call with post=false once at the beginning from a
     * context that can afford to wait, make sure through external means that no
     * new data is added to the cache, and then finally call once with post=true
     * to cleanup anything that was added after the prev call.
     *
     * Note: Since truncate() does not return with the chunkmap lock held, a
     *       get() call done right after truncate() returns can add new data to
     *       the truncated region. Caller should make sure through some other
     *       means that new data is not added.
     *
     * Returns number of chunks that must have been deleted or trimmed, but
     * were skipped as they were inuse. Since VFS writes are blocked during
     * truncate those chunks could be in use only because of some ongoing
     * read calls. Caller will typically wait and call truncate() again, till
     * it returns 0, indicating that all chunks in the region have been
     * truncated.
     * bytes_truncated is the number of bytes dropped from the cache to serve
     * this truncate request.
     */
    int truncate(uint64_t trunc_len, bool post,
                 uint64_t& bytes_truncated);

    /*
     * Returns all dirty chunks for a given range in chunkmap.
     * Before returning it increases the inuse count of underlying membuf(s).
     * Caller will typically sync dirty membuf to Blob and once done must call
     * clear_inuse().
     *
     * Note: The returned membuf(s) were found dirty when get_dirty_bc_range()
     *       scanned the chunmap while holding the chunkmap lock. They may or
     *       may not be flushing.
     *       Since returned membuf(s) are not locked by get_dirty_bc_range(),
     *       some other thread may already be flushing them or may start
     *       flushing anytime after the call returns, hence the caller MUST
     *       check for that after holding the membuf lock, before it tries to
     *       flush those membuf(s).
     */
    std::vector<bytes_chunk> get_dirty_bc_range(
            uint64_t st_off = 0, uint64_t end_off = UINT64_MAX) const;

    /*
     * Returns dirty chunks which are not already flushing, in the given range,
     * from chunkmap. If bytes pointer is passed it's populated with the total
     * number of bytes included in the returned bytes_chunk vector.
     * Before returning it increases the inuse count of underlying membuf(s).
     * Caller will typically sync dirty membuf to Blob and once done must call
     * clear_inuse().
     *
     * Note: Caller MUST call get_dirty_nonflushing_bcs_range() with flush_lock
     *       held.
     *       This ensures that none of the chunks returned starts flushing by
     *       any other thread (as any new flush will wait for the flush_lock).
     *       flush_lock can be released after sync_membufs() is called for the
     *       returned chunks. It'll set dirty membufs to flushing, and issue
     *       write_rpc. These chunks won't be returned by a subsequent call to
     *       get_dirty_nonflushing_bcs_range().
     */
    std::vector<bytes_chunk> get_dirty_nonflushing_bcs_range(
            uint64_t st_off = 0, uint64_t end_off = UINT64_MAX,
            uint64_t *bytes = nullptr) const;

    /*
     * Returns all dirty chunks which are currently flushing for a given range
     * in chunkmap. Before returning it increases the inuse count of underlying
     * membuf(s).
     * Caller will typically wait for these membuf(s) to complete flushing to
     * Blob and once done must call clear_inuse().
     *
     * Note: The returned membuf(s) were found flushing when get_flushing_bc_range()
     *       scanned the chunmap while holding the chunkmap lock.
     *       Since returned membuf(s) are not locked by get_flushing_bc_range(),
     *       they may complete flushing and (optionally) marked commit pending
     *       if they were flushed using UNSTABLE write, hence the caller MUST
     *       check for that after holding the membuf lock, before it tries to
     *       commit those membuf(s).
     */
    std::vector<bytes_chunk> get_flushing_bc_range(
            uint64_t st_off = 0, uint64_t end_off = UINT64_MAX) const;

    /**
     * Returns contiguous dirty (and not flushing) chunks from chunmap, starting
     * with the lowest dirty offset, and returns the total number of (dirty)
     * bytes contained in the returned chunks.
     * Before returning it increases the inuse count of underlying membuf(s).
     * Caller will typically flush these to the backing Blob as UNSTABLE
     * writes.
     */
    std::vector<bytes_chunk> get_contiguous_dirty_bcs(
            uint64_t *bytes = nullptr) const;

    /*
     * Returns *all* commit pending chunks in chunkmap.
     * Before returning it increases the inuse count of underlying membuf(s)
     * and sets the membufs locked. Caller will typically commit the returned
     * membuf(s) to Blob and once done must call clear_commit_pending,
     * clear_locked() and clear_inuse() in that order.
     *
     * Note: We don't have use case where we need to commit for given range as
     *       server on commiting delete all the temporary Put Blocks.
     *
     * Note: The returned membuf(s) were found pending commit when
     *       get_commit_pending_bcs() scanned the chunmap while holding the
     *       chunkmap lock. Since returned membuf(s) are not locked by
     *       get_commit_pending_bcs(), some other thread may already start
     *       committing them anytime after the call returns, hence the caller
     *       MUST check for that after holding the membuf lock, before it tries
     *       to commit those membuf(s).
     */
    std::vector<bytes_chunk> get_commit_pending_bcs(
            uint64_t *bytes = nullptr) const;

    /**
     * Drop cached data in the given range.
     * This must be called only for file-backed caches. For non file-backed
     * caches this is a no-op.
     *
     * Returns the number of bytes reclaimed by dropping the cache. A -ve
     * return indicates error in munmap().
     *
     * Note: It might make sense to not call drop() at all and leave all chunks
     *       mmap()ed at all times, and depend on kernel to manage the buffer
     *       cache. If kernel drops some part of the buffer cache, subsequent
     *       users of that byte range would cause a page fault and kernel will
     *       silently load data from the backing file.
     *       Another approach would be to use mlock() to lock buffer cache data
     *       that we want and let drop() munlock() it so that kernel can choose
     *       to free it. This needs to be tested.
     *       The advantage of having drop support is that we can choose to
     *       drop specific file caches, which are less/not used, and leave the
     *       more actively used caches mapped. Kernel won't have this knowledge
     *       and it can flush any of the file caches under memory pressure.
     */
    int64_t drop(uint64_t offset, uint64_t length);

    /**
     * Clear the cache by releasing all chunks from the cache.
     * For file-backed cache, this also releases all the file blocks.
     * This will be called for invalidating the cache for a file, typically
     * when we detect that file has changed (through getattr or preop attrs
     * telling that mtime is different than what we have cached) or when
     * the file inode is forgotten by fuse and we don't want to keep the cache
     * anymore. For the latter case, clear_nolock() must be called with
     * shutdown param as true.
     *
     * When shutdown is false, following chunks won't be released.
     * - Which are inuse.
     *   These may have ongoing IOs, so not safe to release.
     * - Which are dirty.
     *   These need to be flushed to the Blob, else we lose data.
     * When shutdown is true it means the caller wants to purge the cache and
     * the file is no longer being used, so we release all chunks irrespective
     * of their current state.
     */
    void clear_nolock(bool shutdown = false);

    void clear(bool shutdown = false)
    {
        const std::unique_lock<std::mutex> _lock(chunkmap_lock_43);
        clear_nolock(shutdown);
    }

    /**
     * Mark the cache as invalid.
     * Should be called when it's determined that the cached data is not valid.
     * Depending on the requirement, caller may want to sync the dirty data
     * with the sever before prceeding.
     */
    void invalidate()
    {
        invalidate_pending = true;
    }

    /**
     * Atomically clear invalidate_pending and return the old value.
     */
    bool test_and_clear_invalidate_pending()
    {
        return invalidate_pending.exchange(false);
    }

    /**
     * Drop memory cache for all chunks in this bytes_chunk_cache.
     * Chunks will be loaded as user calls get().
     *
     * Returns the number of bytes reclaimed by dropping the cache. A -ve
     * return indicates error in munmap().
     *
     * See discussion in drop().
     */
    int64_t dropall()
    {
        return drop(0, UINT64_MAX);
    }

    bool is_file_backed() const
    {
        return !backing_file_name.empty();
    }

    /**
     * Maximum size a dirty extent can grow before we should flush it.
     * This is 60% of the allowed cache size or 1GB whichever is lower.
     * The reason for limiting it to 1GiB (or 10 x AZNFSC_MAX_BLOCK_SIZE) is
     * because there's not much value in holding more data than the Blob
     * NFS server's scheduler cache size. We want to send as prompt as
     * possible to utilize the n/w b/w but slow enough to give the write
     * scheduler an opportunity to merge better.
     * For unstable writes this allows us enough PB parallelism.
     */
    static uint64_t max_dirty_extent_bytes();

    /**
     * Get the amount of dirty data that needs to be flushed.
     * This excludes the data which is already flushing.
     * Note that once a thread starts flushing one or more membufs the dirty
     * counter doesn't reduce till the writes complete but another thread
     * looking to flush should not account for those as they are already
     * being flushed.
     *
     * When called with inode->flush_lock() held, bytes to flush can go up
     * after get_bytes_to_flush() returns but it cannot go down.
     * This is because with flush_lock held no new flush can start and hence
     * bytes_flushing cannot go up, but as ongoing flushes complete,
     * bytes_flushing can go down. Otoh, bytes_dirty can go up as
     * copy_to_cache() doesn't need the flush_lock.
     */
    uint64_t get_bytes_to_flush() const
    {
        /*
         * Since we call clear_dirty() before clear_flushing(), we can have
         * bytes_dirty < bytes_flushing, hence we need the protection.
         */
        return std::max((int64_t)(bytes_dirty - bytes_flushing), int64_t(0));
    }

    /**
     * Get the amount of data which has been written as unstable writes, but not
     * yet committed. This excludes dirty data which is not flushed/written yet
     * or in process of flushing. It gets incremented on write completion of
     * dirty data flushed to Blob with unstable parameter.
     *
     * Note: The number of bytes to commit may change after the call returns.
     *       Caller should use it as a hint, and not as a hard limit.
     */
    uint64_t get_bytes_to_commit() const
    {
        assert(bytes_commit_pending <= AZNFSC_MAX_FILE_SIZE);
        return bytes_commit_pending;
    }

    /**
     * Returns true if one or more membufs are being currently flushed to the
     * backing Blob.
     *
     * Note: is_flushing_in_progress() should be called with flush_lock() held.
     *
     * Note: is_flushing_in_progress() can return false positives, i.e., it can
     *       return true as bytes_flushing can change anytime after this call
     *       returns, but it never returns false negatives, i.e., it never
     *       returns false when bytes_flushing is non-zero.
     */
    bool is_flushing_in_progress() const
    {
        return bytes_flushing > 0;
    }

    /**
     * Maximum size of commit_pending data that can be in cache, before we
     * must commit it to Blob.
     * It should be greater than or equal to the flush threshold (as returned
     * by max_dirty_extent_bytes()) and smaller than the inline write threshold
     * (as suggested by do_inline_write()), to minimize inline flush waits as
     * much as possible, in steady state.
     */
    static uint64_t max_commit_bytes()
    {
        // Maximum cache size allowed in bytes.
        static const uint64_t max_total =
            (aznfsc_cfg.cache.data.user.max_size_mb * 1024 * 1024ULL);
        assert(max_total != 0);

        /*
         * Minimum of 60% of max cache and 2 times the flush limit.
         * We want to commit as soon as possible w/o affecting performance.
         * If we commit too often, since commit is a serializing operation,
         * it'll affect the write throughput, otoh, if we commit too late
         * then we might hit the inline write threshold, which again would
         * serialize writes, bringing down throughput.
         */
        // Capped due to global cache size.
        static const uint64_t max_commit_bytes_g = (max_total * 0.6);
        // Capped due to per-file cache discipline.
        static const uint64_t max_commit_bytes_l = 2 * max_dirty_extent_bytes();
        static const uint64_t max_commit_bytes =
            std::min(max_commit_bytes_g, max_commit_bytes_l);

        // At least one full sized block.
        assert(max_commit_bytes >= AZNFSC_MAX_BLOCK_SIZE);

        return max_commit_bytes;
    }

    /**
     * Check if we must initiate a COMMIT RPC now. Note that the caller would
     * just send the COMMIT RPC and not necessarily block the user write
     * request till the COMMIT RPC completes, i.e., it's not an inline commit.
     *
     * We must start commit if:
     * 1. We have enough commit_pending data for this file/cache, or,
     * 2. Global memory pressure dictates that we commit now to free up
     *    memory. In this case we might be committing more frequently which
     *    won't necessarily be optimal, but we have no choice due to the
     *    memory pressure.
     */
    bool commit_required() const
    {
        const bool local_pressure =
            (bytes_commit_pending >= max_commit_bytes());

        if (local_pressure) {
            INC_GBL_STATS(commit_lp, 1);
            return true;
        }

        /*
         * TODO: Take cue from global memory pressure.
         */
        return false;
    }

    /**
     * Check if we must initiate flush of some cached data. Note that the caller
     * would just send the corresponding WRITE RPC and not necessarily block the
     * user write request till the WRITE RPC completes, i.e., it's not an inline
     * write.
     *
     * We must start flush/write if:
     * 1. We have enough bytes to flush so that we can write a full sized
     *    block, or for the case of stable write, we have enough data to fill
     *    the scheduler queue.
     * 2. Global memory pressure dictates that we flush now to free up memory.
     *    In this case we might be flushing more frequently which won't
     *    necessarily be optimal, but we have no choice due to the memory
     *    pressure.
     */
    bool flush_required(uint64_t extent_size = 0) const
    {
        static const uint64_t mdeb = max_dirty_extent_bytes();

        if (extent_size >= mdeb) {
            INC_GBL_STATS(flush_seq, 1);
            return true;
        }

        const bool local_pressure = (get_bytes_to_flush() >= mdeb);

        if (local_pressure) {
            INC_GBL_STATS(flush_lp, 1);
            return true;
        }

        /*
         * TODO: Take cue from global memory pressure.
         */
        return false;
    }

    /**
     * This should be called by writer threads to find out if they must wait
     * for the write to complete. This will check both the cache specific and
     * global memory pressure.
     */
    bool do_inline_write() const
    {
        /*
         * Allow four full-sized dirty extents before we force inline write.
         * This 4GB dirty data could be sitting in any combination of commit
         * pending, flushing and dirty. Let's see how the flush, commit and
         * dirty bytes would play out.
         *
         * e.g.,
         * if the max_dirty_extent_bytes() is 1GB, then we have
         * flush_required() @ 1GB
         * commit_required() @ 2GB
         * do_inline_write() @ 4GB.
         *
         * Assuming backend flush speed of 1GB/s and memory write speed of
         * 5GB/s and typical commit time of ~50ms, we have the following
         * derived facts:
         * - flush of 1GB extent will take ~1sec.
         * - during this time application can dirty 5GB more data, so we
         *   are likely to hit inline write limit even before the flush
         *   completed and commit starts. If we consider slightly lesser
         *   write speeds of say 2GB/s, then by the time the flush completes
         *   we have dirtied another 2GB. Now flush for the next 1GB extent
         *   will start and it'll take another 1sec and by this time writes
         *   can dirty another 2GB, so inline write limit will hit.
         *
         * This means,
         * till [0-1GB] dirty data, we don't trigger any flushing/committing and
         * of course we don't hold application writes for inline writes.
         * The first write to hit 1GB dirty data, triggers flushing of the entire
         * 1GB extent through multiple block sized flushes. The write itself,
         * and further writes are not blocked.
         * When the 1GB extent completes flushing, the extent is marked commit
         * pending and if another 1GB dirty extent is accumulated we start
         * flushing that extent. If the next 1GB dirty extent is not yet ready,
         * we do nothing. When the 1GB dirty extent is ready we initiate
         * flushing and when this completes flushing, we have 2GB commit pending
         * and hence commit is triggered. No writes are blocked till now, and
         * they can dirty 2 more 1GB extents.
         * Commit typically takes around 50ms to complete. For RAM write speed
         * of say 5GB/s, this would mean application can dirty 250MB more till
         * the commit completes.
         * More application writes will cause more dirty data. If the commit
         * completes, then we have more free space and we don't need writes to
         * wait inline, else we hit the inline limit and writes are blocked.
         */
        static const uint64_t max_dirty_allowed_per_file =
            max_dirty_extent_bytes() * 4;
        const bool local_pressure =
            (bytes_dirty + bytes_commit_pending) > max_dirty_allowed_per_file;

        /*
         * TODO: Add counter/stats for counting how many times we forced
         *       inline write due to local and how many times due to global
         *       reasons.
         */
        if (local_pressure) {
            INC_GBL_STATS(inline_writes_lp, 1);
            return true;
        }

        /*
         * Global pressure is when get_prune_goals() returns non-zero bytes
         * to be pruned inline.
         */
        uint64_t inline_bytes;

        get_prune_goals(&inline_bytes, nullptr);
        const bool global_pressure = (inline_bytes > 0);

        if (global_pressure) {
            INC_GBL_STATS(inline_writes_gp, 1);
            return true;
        }

        return false;
    }

    /**
     * get_prune_goals() looks at the following information and returns prune
     * goals for this cache:
     * - Total memory consumed by all caches.
     * - aznfsc_cfg.cache_max_mb (maximum total cache size allowed).
     * - Memory consumed by this particular cache.
     *
     * It returns two types of prune goals:
     * - Inline.
     *   This tells how much memory to free inline.
     *   This will be non-zero only under extreme memory pressure where we
     *   cannot let writers continue w/o making space.
     * - Periodic.
     *   This tells how much memory to free by the periodic sync thread.
     *   In most common cases this is how memory will be reclaimed.
     */
    void get_prune_goals(uint64_t *inline_bytes, uint64_t *periodic_bytes) const
    {
        // Maximum cache size allowed in bytes.
        static const uint64_t max_total =
            (aznfsc_cfg.cache.data.user.max_size_mb * 1024 * 1024ULL);
        assert(max_total != 0);

        /*
         * If cache usage grows to 90% of max, we enforce inline pruning for
         * writers. When cache usage grows more than 70% we recommend periodic
         * pruning. If the cache size is sufficient, hopefully we will not need
         * inline pruning too often, as it hurts application write performance.
         * Once curr_bytes_total exceeds inline_threshold we need to perform
         * inline pruning. We prune all the way upto inline_target to avoid
         * hysteresis. Similarly for periodic pruning we prune all the way
         * upto periodic_target.
         *
         * Following also means that at any time, half of the cache_max_mb
         * can be safely present in the cache.
         */
        static const uint64_t inline_threshold = (max_total * 0.9);
        static const uint64_t inline_target = (max_total * 0.8);
        static const uint64_t periodic_threshold = (max_total * 0.7);
        static const uint64_t periodic_target = (max_total * 0.6);

        /*
         * Current total cache size in bytes. Save it once to avoid issues
         * with bytes_allocated* changing midway in these calculations.
         */
        const uint64_t curr_bytes_total = bytes_allocated_g;
        const uint64_t curr_bytes = bytes_allocated;

        if (inline_bytes) {
            *inline_bytes = 0;
        }

        if (periodic_bytes) {
            *periodic_bytes = 0;
        }

        /*
         * If current cache usage is more than the inline_threshold limit, we
         * need to recommend inline pruning. We calculate how much %age of
         * total caches we need to prune and then divide it proportionately
         * among various caches (bigger caches need to prune more). We prune
         * upto inline_target;
         */
        if (inline_bytes && (curr_bytes_total > inline_threshold)) {
            assert(inline_threshold > inline_target);

            // How much to prune?
            const uint64_t total_inline_goal =
                (curr_bytes_total - inline_target);
            const double percent_inline_goal =
                (total_inline_goal * 100.0 ) / curr_bytes_total;

            *inline_bytes = (curr_bytes * percent_inline_goal) / 100;

            // Prune at least 1MB.
            if (*inline_bytes < 1048576) {
                *inline_bytes = 1048576;
            }
        }

        if (periodic_bytes && (curr_bytes_total > periodic_threshold)) {
            assert(periodic_threshold > periodic_target);

            const uint64_t total_periodic_goal =
                (curr_bytes_total - periodic_target);
            const double percent_periodic_goal =
                (total_periodic_goal * 100.0 ) / curr_bytes_total;

            *periodic_bytes = (curr_bytes * percent_periodic_goal) / 100;

            if (*periodic_bytes < 1048576) {
                *periodic_bytes = 1048576;
            }
        }
    }

    /**
     * Check and perform inline pruning if needed.
     * We do inline pruning when we are "extremely" high on memory usage and
     * hence cannot proceed w/o making space for this new request. This must be
     * called from get() which may need more memory.
     *
     * TODO: Also add periodic pruning support.
     */
    void inline_prune();

    /**
     * This will run self tests to test the correctness of this class.
     */
    static int unit_test();

    /*
     * Stats for this cache.
     * These have been made public for easy access, w/o needing whole bunch
     * of accessor methods. Don't update them from outside!
     *
     * bytes_allocated is the total number of memmory bytes allocated for all
     * the bytes_chunk in this cache. Note that all of that memory may not be
     * used for cacheing and bytes_cached is the total bytes actually used for
     * cacheing. Following are the cases where allocated would be larger than
     * used:
     * - release() may release parts of the cache, though membuf cannot be
     *   freed till the entire membuf is unused.
     * - For file-backed cache we have to mmap() on a 4k granularity but the
     *   actual bytes_chunk may not be 4k granular.
     *
     * bytes_cached tracks the total number of bytes cached, not necessarily
     * in memory. For file-backed cache, bytes_cached may refer to memory bytes
     * or file bytes. Note that bytes_cached is not reduced when membuf is
     * drop()ped. This is because the data is still cached, albeit in the
     * backing file.
     */
    std::atomic<uint64_t> num_chunks = 0;
    std::atomic<uint64_t> num_get = 0;
    std::atomic<uint64_t> bytes_get = 0;
    std::atomic<uint64_t> num_release = 0;
    std::atomic<uint64_t> bytes_release = 0;
    std::atomic<uint64_t> num_truncate = 0;
    std::atomic<uint64_t> bytes_truncate = 0;
    std::atomic<uint64_t> bytes_allocated = 0;
    std::atomic<uint64_t> bytes_cached = 0;
    std::atomic<uint64_t> bytes_dirty = 0;
    std::atomic<uint64_t> bytes_flushing = 0;
    std::atomic<uint64_t> bytes_commit_pending = 0;
    std::atomic<uint64_t> bytes_uptodate = 0;
    std::atomic<uint64_t> bytes_inuse = 0;
    std::atomic<uint64_t> bytes_locked = 0;

    /*
     * Global stats for all caches.
     */
    static std::atomic<uint64_t> num_chunks_g;
    static std::atomic<uint64_t> num_get_g;
    static std::atomic<uint64_t> bytes_get_g;
    static std::atomic<uint64_t> num_release_g;
    static std::atomic<uint64_t> bytes_release_g;
    static std::atomic<uint64_t> num_truncate_g;
    static std::atomic<uint64_t> bytes_truncate_g;
    static std::atomic<uint64_t> bytes_allocated_g;
    static std::atomic<uint64_t> bytes_cached_g;
    static std::atomic<uint64_t> bytes_dirty_g;
    static std::atomic<uint64_t> bytes_flushing_g;
    static std::atomic<uint64_t> bytes_commit_pending_g;
    static std::atomic<uint64_t> bytes_uptodate_g;
    static std::atomic<uint64_t> bytes_inuse_g;
    static std::atomic<uint64_t> bytes_locked_g;
    // How many times set_locked() was called;
    static std::atomic<uint64_t> num_locked_g;
    // How many times set_locked() had to wait.
    static std::atomic<uint64_t> num_lockwait_g;
    // How much cumulative usecs those calls had to wait.
    static std::atomic<uint64_t> lock_wait_usecs_g;

    static uint64_t get_num_caches()
    {
        return num_caches;
    }

    struct nfs_inode *get_inode() const
    {
        return inode;
    }

    /**
     * Cache size is defined as 1+ offset of the last uptodate byte.
     *
     * Note: It can change anytime after this, so use it only as an estimate
     *       which can be stale.
     */
    uint64_t get_cache_size() const
    {
        assert(cache_size <= AZNFSC_MAX_FILE_SIZE);
        /*
         * bytes_uptodate is a count while cache_size is an offset.
         * set_uptodate() updates cache_size before bytes_uptodate so we can
         * safely assert for this. Also truncate() reduces cache_size after
         * reducing bytes_uptodate.
         *
         * Note: If read races with truncate we can have truncate() release
         *       the membuf(s) but its destructor may not be called hence
         *       bytes_uptodate may be reduced after cache_size causing this
         *       assert to fail, but that should be rare so leave the assert.
         *       The 2nd part of the assert is to protect against the above.
         *       bytes_cached is decremented in truncate() when we remove a
         *       chunk from the chunkmap, but bytes_uptodate is reduced only
         *       whem membuf destructor is called which happens only later
         *       in this case. bytes_uptodate can usually not be greater than
         *       bytes_cached but in this case it can be and hence that covers
         *       the exception case.
         *
         *       XXX If ~membuf() is called between the two checks, then the
         *           assert will fail, but that's very unlikely and assert is
         *           o/w valuable.
         */
        assert((cache_size >= bytes_uptodate) ||
               (bytes_uptodate > bytes_cached));
        return cache_size;
    }

private:

    /**
     * Scan all chunks lying in the range [offset, offset+length) and perform
     * requested action, as described below:
     *
     * SCAN_ACTION_GET     -> Return list of chunks covering the requested
     *                        range, allocating non-existent chunks and adding
     *                        to chunkmap. If extent_left/extent_right are non
     *                        null, they contain the left and right edge of the
     *                        contiguous extent that contains [offset, offset+length).
     * SCAN_ACTION_RELEASE -> Free chunks contained in the requested range.
     *
     * bytes_released will be set to the number of bytes actually released,
     * i.e., either entire chunk was released (and membuf freed) or the chunk
     * was trimmed.
     */
    std::vector<bytes_chunk> scan(uint64_t offset,
                                  uint64_t length,
                                  scan_action action,
                                  uint64_t *bytes_released = nullptr,
                                  uint64_t *extent_left = nullptr,
                                  uint64_t *extent_right = nullptr);

    /**
     * This must be called with bytes_chunk_cache lock held.
     */
    bool extend_backing_file(uint64_t newlen)
    {
        // No-op for non file-backed caches.
        if (backing_file_fd == -1) {
            assert(backing_file_len == 0);
            return true;
        }

        assert(newlen > 0);
        assert(newlen <= AZNFSC_MAX_FILE_SIZE);
        assert(backing_file_fd > 0);

        if (backing_file_len < newlen) {
            const int ret = ::ftruncate(backing_file_fd, newlen);
            if (ret != 0) {
                AZLogError("ftruncate(fd={}, length={}) failed: {}",
                           backing_file_fd, newlen, strerror(errno));
                assert(0);
                return false;
            }

            backing_file_len = newlen;
        }

        return true;
    }

    /*
     * std::map of bytes_chunk, indexed by the starting offset of the chunk.
     */
    std::map<uint64_t, struct bytes_chunk> chunkmap;

    // Lock to protect chunkmap.
    mutable std::mutex chunkmap_lock_43;

    /*
     * Size of the cache.
     * This is 1+ offset of the last uptodate byte seen by this cache.
     * Increased in set_uptodate() and decreased *only* in truncate() iff
     * truncate shrinks the file size. Note that this reflects the maximum
     * cache size that we had till now and not necessarily what the current
     * cache holds. IOW, release()/clear()/inline_prune() may release one or
     * more chunks thus removing the actual cache contents, but it doesn't
     * reduce the cache_size.
     *
     * Note: It should actually be called cached_filesize.
     */
    std::atomic<uint64_t> cache_size = 0;

    /*
     * File whose data we are cacheing.
     * Note that we don't hold a ref on this inode so it's only safe to use
     * from inline_prune() where we know inode is active.
     *
     * XXX If you use it from some other place either make sure inode is
     *     safe to use from there or hold a ref on the inode.
     */
    struct nfs_inode *const inode;

    std::string backing_file_name;
    int backing_file_fd = -1;
    std::atomic<uint64_t> backing_file_len = 0;

    /*
     * Flag to quickly mark the cache as invalid w/o purging the entire
     * cache. Once invalidate_pending is set, next cache lookup will first
     * purge the cache before proceeding.
     */
    std::atomic<bool> invalidate_pending = false;

    // Count of total active caches.
    static std::atomic<uint64_t> num_caches;
};

}

#endif /* __AZNFSC_FILE_CACHE_H__ */
