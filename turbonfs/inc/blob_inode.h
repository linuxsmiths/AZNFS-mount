#ifndef __BLOB_INODE_H__
#define __BLOB_INODE_H__

#include <azure/storage/blobs.hpp>
#include <string>
#include <memory>
#include <mutex>
#include "aznfsc.h"

/**
 * Minimal structure to represent a Blob inode in FUSE.
 */
struct blob_inode {

    /*
     * S_IFREG, S_IFDIR, etc.
     * 0 is not a valid file type.
     */
    const uint32_t file_type = 0;
    
    struct blob_client *const client;
    fuse_ino_t ino;  // FUSE inode number
    std::string blob_name;  // Blob name in Azure Storage
    uint64_t size;  // Blob size in bytes
    std::string last_modified;  // Last modified timestamp
    std::string etag;  // ETag for cache validation
    mutable std::atomic<uint64_t> lookupcnt = 0;
    mutable std::atomic<uint64_t> dircachecnt = 0;
    /*
     * How many open fds for this file are currently present in fuse.
     * Incremented when fuse calls open()/creat().
     */
    std::atomic<uint64_t> opencnt = 0;

    mutable std::mutex ilock;  // Protect inode updates

    // Track expected forget count for this inode
    mutable std::atomic<uint64_t> forget_expected = 0;

    // Track last forget timestamp for logging
    mutable uint64_t last_forget_seen_usecs = 0;

    // Constructor without default arguments
    blob_inode(const uint32_t _file_type, struct blob_client *_client, fuse_ino_t inode, const std::string& name);

    ~blob_inode();

    fuse_ino_t get_fuse_ino() const
    {
        assert(ino != 0);
        return ino;
    }

    bool is_dir() const
    {
        return (file_type == S_IFDIR);
    }

    // Is regular file?
    bool is_regfile() const
    {
        return (file_type == S_IFREG);
    }

    /**
     * Short character code for file_type, useful for logs.
     */
    char get_filetype_coding() const
    {
#ifndef ENABLE_NON_AZURE_NFS
        assert(file_type == S_IFDIR ||
               file_type == S_IFREG ||
               file_type == S_IFLNK);
#endif
        return (file_type == S_IFDIR) ? 'D' :
               ((file_type == S_IFLNK) ? 'S' :
                ((file_type == S_IFREG) ? 'R' : 'U'));
    }

    void on_fuse_open(enum fuse_opcode optype)
    {
        /*
         * Only these fuse ops correspond to open()/creat() which return an
         * fd.
         */
        assert((optype == FUSE_CREATE) ||
               (optype == FUSE_OPEN) ||
               (optype == FUSE_OPENDIR));

        opencnt++;

        AZLogDebug("[{}:{}] on_fuse_open({}), new opencnt is {}",
                   get_filetype_coding(), ino, (int) optype, opencnt.load());

    }


    /**
     * Increment lookupcnt of the inode.
     */
    void incref() const
    {
        lookupcnt++;

        AZLogDebug("[{}] lookupcnt incremented to {} (dircachecnt: {}, "
                   "forget_expected: {})",
                   ino, lookupcnt.load(), dircachecnt.load(),
                   forget_expected.load());
    }

    /**
     * Decrement lookupcnt of the inode and delete it if lookupcnt
     * reaches 0.
     * 'cnt' is the amount by which the lookupcnt must be decremented.
     * This is usually the nlookup parameter passed by fuse FORGET, when
     * decref() is called from fuse FORGET, else it's 1.
     * 'from_forget' should be set to true when calling decref() for
     * handling fuse FORGET.
     */
    void decref(size_t cnt, bool forget_expected);

    /**
     * Returns true if inode is FORGOTten by fuse.
     * Forgotten inodes will not be referred by fuse in any api call.
     */
    bool is_forgotten() const
    {
        return (lookupcnt == 0);
    }

    int flush_cache_and_wait()
    {
        return 0;
    }
};

#endif /* __BLOB_INODE_H__ */
