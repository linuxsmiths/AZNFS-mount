#include "nfs_internal.h"
#include "rpc_task.h"
#include "nfs_client.h"
#include "blob_client.h"
#include "rpc_stats.h"
#include <azure/identity.hpp>
#include <azure/storage/blobs.hpp>

/*
 * Catch incorrect use of alloc_rpc_task() in libnfs callbacks.
 */
#define alloc_rpc_task use_alloc_rpc_task_reserved_in_libnfs_callback

/*
 * If this is defined we will call release() for the byte chunk which is read
 * by the application. This helps free the cache as soon as the reader reads
 * it. The idea is to not keep cached data hanging around for any longer than
 * it's needed. Most common access pattern that we need to support is seq read
 * where readahead fills the cache and helps future application read calls.
 * Once application has read the data, we don't want it to linger in the cache
 * for any longer. This means future reads won't get it, but the idea is that
 * if future reads are also sequential, they will get it by readahead.
 *
 * If this is disabled, then pruning is the only way to reclaim cache memory.
 */
#define RELEASE_CHUNK_AFTER_APPLICATION_READ

#ifdef ENABLE_PRESSURE_POINTS
#define INJECT_JUKEBOX(res, task) \
do { \
    if (res && (NFS_STATUS(res) == NFS3_OK)) { \
        if (inject_error()) { \
            if (task->rpc_api->is_dirop()) { \
                AZLogWarn("[{}/{}] PP: {} jukebox", \
                          task->rpc_api->get_parent_ino(), \
                          task->rpc_api->get_file_name(), \
                          __FUNCTION__); \
            } else { \
                AZLogWarn("[{}] PP: {} jukebox", \
                          task->rpc_api->get_ino(), __FUNCTION__); \
            } \
            (res)->status = NFS3ERR_JUKEBOX; \
        } \
    } \
} while (0)
#else
#define INJECT_JUKEBOX(res, task) /* nothing */
#endif

#ifdef ENABLE_PRESSURE_POINTS
#define INJECT_BAD_COOKIE(res, task) \
do { \
    /* \
     * Must be called only for READDIR and READDIRPLUS. \
     */ \
    assert(task->get_op_type() == FUSE_READDIR || \
           task->get_op_type() == FUSE_READDIRPLUS); \
    if (res && (NFS_STATUS(res) == NFS3_OK)) { \
        /* \
         * Don't simulate badcookie error for cookie==0. \
         */ \
        if (inject_error() && \
            task->rpc_api->readdir_task.get_offset() != 0) { \
            AZLogWarn("[{}] PP: {} bad cookie, offset {}, target_offset {} ", \
                       task->rpc_api->readdir_task.get_ino(), \
                       __FUNCTION__, \
                       task->rpc_api->readdir_task.get_offset(), \
                       task->rpc_api->readdir_task.get_target_offset()); \
            (res)->status = NFS3ERR_BAD_COOKIE; \
        } \
    } \
} while (0)
#else
#define INJECT_BAD_COOKIE(res, task) /* nothing */
#endif

#ifdef ENABLE_PRESSURE_POINTS
#define INJECT_CREATE_FH_POPULATE_FAILURE(res, task) \
do { \
    /* \
     * Must be called only for create task. \
     */ \
    assert(task->get_op_type() == FUSE_CREATE); \
    if (res && (NFS_STATUS(res) == NFS3_OK) && \
        (res)->CREATE3res_u.resok.obj.handle_follows) { \
        if (inject_error()) { \
            AZLogWarn("PP: {} failed to populate fh, parent_ino: {}, " \
                      "filename: {}", \
                      __FUNCTION__, \
                      task->rpc_api->create_task.get_parent_ino(), \
                      task->rpc_api->create_task.get_file_name()); \
            (res)->CREATE3res_u.resok.obj.handle_follows = 0; \
        } \
    } \
} while (0)

#define INJECT_MKNOD_FH_POPULATE_FAILURE(res, task) \
do { \
    /* \
     * Must be called only for mknod task. \
     */ \
    assert(task->get_op_type() == FUSE_MKNOD); \
    if (res && (NFS_STATUS(res) == NFS3_OK) && \
        (res)->CREATE3res_u.resok.obj.handle_follows) { \
        if (inject_error()) { \
            AZLogWarn("PP: {} failed to populate fh, parent_ino: {}, " \
                      "filename: {}", \
                      __FUNCTION__, \
                      task->rpc_api->mknod_task.get_parent_ino(), \
                      task->rpc_api->mknod_task.get_file_name()); \
            (res)->CREATE3res_u.resok.obj.handle_follows = 0; \
        } \
    } \
} while (0)

#define INJECT_MKDIR_FH_POPULATE_FAILURE(res, task) \
do { \
    /* \
     * Must be called only for mkdir task. \
     */ \
    assert(task->get_op_type() == FUSE_MKDIR); \
    if (res && (NFS_STATUS(res) == NFS3_OK) && \
        (res)->MKDIR3res_u.resok.obj.handle_follows) { \
        if (inject_error()) { \
            AZLogWarn("PP: {} failed to populate fh, parent_ino: {}, " \
                      "dirname: {}", \
                      __FUNCTION__, \
                      task->rpc_api->mkdir_task.get_parent_ino(), \
                      task->rpc_api->mkdir_task.get_dir_name()); \
            (res)->MKDIR3res_u.resok.obj.handle_follows = 0; \
        } \
    } \
} while (0)

#define INJECT_SYMLINK_FH_POPULATE_FAILURE(res, task) \
do { \
    /* \
     * Must be called only for symlink task. \
     */ \
    assert(task->get_op_type() == FUSE_SYMLINK); \
    if (res && (NFS_STATUS(res) == NFS3_OK) && \
        (res)->SYMLINK3res_u.resok.obj.handle_follows) { \
        if (inject_error()) { \
            AZLogWarn("PP: {} failed to populate fh, parent_ino: {}, " \
                      "dirname: {}", \
                      __FUNCTION__, \
                      task->rpc_api->symlink_task.get_parent_ino(), \
                      task->rpc_api->symlink_task.get_name()); \
            (res)->SYMLINK3res_u.resok.obj.handle_follows = 0; \
        } \
    } \
} while (0)

#define INJECT_SETATTR_FH_POPULATE_FAILURE(res, task) \
do { \
    /* \
     * Must be called only for setattr. \
     */ \
    assert(task->get_op_type() == FUSE_SETATTR); \
    if (res && (NFS_STATUS(res) == NFS3_OK) && \
        (res)->SETATTR3res_u.resok.obj_wcc.after.attributes_follow) { \
        if (inject_error()) { \
            AZLogWarn("PP: {} failed to populate fh, ino: {}", \
                      __FUNCTION__, \
                      task->rpc_api->setattr_task.get_ino()); \
            (res)->SETATTR3res_u.resok.obj_wcc.after.attributes_follow = 0; \
        } \
    } \
} while (0)
#else
#define INJECT_CREATE_FH_POPULATE_FAILURE(res, task) /* nothing */
#define INJECT_MKNOD_FH_POPULATE_FAILURE(res, task) /* nothing */
#define INJECT_MKDIR_FH_POPULATE_FAILURE(res, task) /* nothing */
#define INJECT_SYMLINK_FH_POPULATE_FAILURE(res, task) /* nothing */
#define INJECT_SETATTR_FH_POPULATE_FAILURE(res, task) /* nothing */
#endif

/* static */
std::atomic<int> rpc_task::async_slots = MAX_ASYNC_RPC_TASKS;

/* static */
const std::string rpc_task::fuse_opcode_to_string(fuse_opcode opcode)
{
    /*
     * We use FUSE_FLUSH to indicate COMMIT RPC.
     */
    if (opcode == FUSE_FLUSH) {
        return "COMMIT";
    }

#define _case(op)   \
    case FUSE_##op: \
        return #op;

    switch (opcode) {
        _case(LOOKUP);
        _case(FORGET);
        _case(GETATTR);
        _case(SETATTR);
        _case(READLINK);
        _case(SYMLINK);
        _case(MKNOD);
        _case(MKDIR);
        _case(UNLINK);
        _case(RMDIR);
        _case(RENAME);
        _case(LINK);
        _case(OPEN);
        _case(READ);
        _case(WRITE);
        _case(STATFS);
        _case(RELEASE);
        _case(FSYNC);
        _case(SETXATTR);
        _case(GETXATTR);
        _case(LISTXATTR);
        _case(REMOVEXATTR);
        _case(FLUSH);
        _case(INIT);
        _case(OPENDIR);
        _case(READDIR);
        _case(RELEASEDIR);
        _case(FSYNCDIR);
        _case(GETLK);
        _case(SETLK);
        _case(SETLKW);
        _case(ACCESS);
        _case(CREATE);
        _case(INTERRUPT);
        _case(BMAP);
        _case(DESTROY);
        _case(IOCTL);
        _case(POLL);
        _case(NOTIFY_REPLY);
        _case(BATCH_FORGET);
        _case(FALLOCATE);
        _case(READDIRPLUS);
        _case(RENAME2);
        _case(LSEEK);
        _case(COPY_FILE_RANGE);
        _case(SETUPMAPPING);
        _case(REMOVEMAPPING);
#if 0
        _case(SYNCFS);
        _case(TMPFILE);
        _case(STATX);
#endif
        default:
            AZLogError("fuse_opcode_to_string: Unknown opcode {}", (int) opcode);
            return "Unknown";
    }
#undef _case
}

void rpc_task::init_lookup(fuse_req *request,
                           const char *name,
                           fuse_ino_t parent_ino)
{
    assert(get_op_type() == FUSE_LOOKUP);
    set_fuse_req(request);
    rpc_api->lookup_task.set_file_name(name);
    rpc_api->lookup_task.set_parent_ino(parent_ino);
    rpc_api->lookup_task.set_fuse_file(nullptr);
}

void rpc_task::do_proxy_lookup() const
{
    AZLogInfo("do_proxy_lookup called");
}

void rpc_task::init_access(fuse_req *request,
                           fuse_ino_t ino,
                           int mask)
{
    AZLogInfo("init_access called");
}

void rpc_task::init_flush(fuse_req *request,
                          fuse_ino_t ino)
{
    assert(get_op_type() == FUSE_FLUSH);
    set_fuse_req(request);
    rpc_api->flush_task.set_ino(ino);

}

void rpc_task::init_write_fe(fuse_req *request,
                          fuse_ino_t ino,
                          struct fuse_bufvec *bufv,
                          size_t size,
                          off_t offset)
{
    AZLogInfo("init_write_fe called");
}

void rpc_task::init_write_be(fuse_ino_t ino)
{
    AZLogInfo("init_write_be called");
}


void rpc_task::init_statfs(fuse_req *request,
                           fuse_ino_t ino)
{
    AZLogInfo("init_statfs called");
}

void rpc_task::init_create_file(fuse_req *request,
                                fuse_ino_t parent_ino,
                                const char *name,
                                mode_t mode,
                                struct fuse_file_info *file)
{
    AZLogInfo("init_create_file called");
}

void rpc_task::init_mknod(fuse_req *request,
                          fuse_ino_t parent_ino,
                          const char *name,
                          mode_t mode)
{
    AZLogInfo("init_mknod called");
}

void rpc_task::init_mkdir(fuse_req *request,
                          fuse_ino_t parent_ino,
                          const char *name,
                          mode_t mode)
{
    AZLogInfo("init_mkdir called");
}

void rpc_task::init_unlink(fuse_req *request,
                           fuse_ino_t parent_ino,
                           const char *name,
                           bool for_silly_rename)
{
    AZLogInfo("init_unlink called");
}

void rpc_task::init_rmdir(fuse_req *request,
                          fuse_ino_t parent_ino,
                          const char *name)
{
    AZLogInfo("init_rmdir called");
}

void rpc_task::init_symlink(fuse_req *request,
                            const char *link,
                            fuse_ino_t parent_ino,
                            const char *name)
{
    AZLogInfo("init_symlink called");
}

/*
 * silly_rename: Is this a silly rename (or user initiated rename)?
 * silly_rename_ino: Fuse inode number of the file being silly renamed.
 *                   Must be 0 if silly_rename is false.
 * For parameters see init_rename() prototype in rpc_task.h.
 */
void rpc_task::init_rename(fuse_req *request,
                           fuse_ino_t parent_ino,
                           const char *name,
                           fuse_ino_t newparent_ino,
                           const char *newname,
                           bool silly_rename,
                           fuse_ino_t silly_rename_ino,
                           fuse_ino_t oldparent_ino,
                           const char *old_name)
{
    AZLogInfo("init_rename called");
}

void rpc_task::init_readlink(fuse_req *request,
                            fuse_ino_t ino)
{
    AZLogInfo("init_readlink called");
}

void rpc_task::init_setattr(fuse_req *request,
                            fuse_ino_t ino,
                            const struct stat *attr,
                            int to_set,
                            struct fuse_file_info *file)
{
    AZLogInfo("init_setattr called");
}

void rpc_task::init_readdir(fuse_req *request,
                            fuse_ino_t ino,
                            size_t size,
                            off_t offset,
                            off_t target_offset,
                            struct fuse_file_info *file)
{
    AZLogInfo("init_readdir called");
}

void rpc_task::init_readdirplus(fuse_req *request,
                                fuse_ino_t ino,
                                size_t size,
                                off_t offset,
                                off_t target_offset,
                                struct fuse_file_info *file)
{
    AZLogInfo("init_readdirplus called");
}

void rpc_task::init_read_fe(fuse_req *request,
                            fuse_ino_t ino,
                            size_t size,
                            off_t offset,
                            struct fuse_file_info *file)
{
    assert(get_op_type() == FUSE_READ);
    set_fuse_req(request);
    rpc_api->read_task.set_ino(ino);
    rpc_api->read_task.set_size(size);
    rpc_api->read_task.set_offset(offset);
    rpc_api->read_task.set_fuse_file(file);

    assert(rpc_api->read_task.is_fe());
    assert(!rpc_api->read_task.is_be());
}

void rpc_task::init_read_be(fuse_ino_t ino,
                            size_t size,
                            off_t offset)
{
    AZLogInfo("init_read_be called");
}


static void lookup_callback(
    struct rpc_context *rpc,
    int rpc_status,
    void *data,
    void *private_data)
{
    rpc_task *task = (rpc_task*) private_data;
    assert(task->magic == RPC_TASK_MAGIC);
    assert(task->rpc_api->optype == FUSE_LOOKUP);

    // Get parent inode and blob name from task
    const fuse_ino_t parent_ino = task->rpc_api->lookup_task.get_parent_ino();
    const std::string blobName = task->rpc_api->lookup_task.get_file_name();

    
    auto parent_inode_ptr = task->get_client()->get_blob_inode_from_ino(parent_ino);
    if (parent_inode_ptr)
    {
        struct blob_inode* parent_inode = parent_inode_ptr.get();
        //AZLogDebug("{}", parent_inode);
        parent_inode->blob_name = blobName;
    }



    auto* blobProperties = static_cast<Azure::Storage::Blobs::Models::BlobProperties*>(data);
    if (!blobProperties) {
        AZLogError("lookup_callback: blobProperties is NULL for parent inode {}", parent_ino);
        task->reply_error(EIO);
        return;
    }

    AZLogDebug("lookup_callback: Blob properties fetched for blob '{}' parent inode {} {}", 
                blobName, 
                parent_ino);

    AZLogDebug("  - Size: {} bytes", blobProperties->BlobSize);
    AZLogDebug("  - Last Modified: {}", blobProperties->LastModified.ToString());

    // Create or retrieve blob_inode
    auto new_inode_ptr = task->get_client()->create_blob_inode(blobName, blobProperties);
    if (!new_inode_ptr) {
        AZLogError("lookup_callback: Failed to create inode for blob '{}'", blobName);
        task->reply_error(EIO);
        return;
    }

    blob_inode* new_inode = new_inode_ptr.get();

    struct fuse_entry_param e {};
    e.ino = new_inode->ino;
    e.attr_timeout = 1.0;
    e.entry_timeout = 1.0;
    e.attr.st_ino = new_inode->ino;
    e.attr.st_mode = S_IFREG | 0644;
    e.attr.st_nlink = 1;
    e.attr.st_size = blobProperties->BlobSize;
    e.attr.st_atime = e.attr.st_mtime = e.attr.st_ctime = time(nullptr);

    task->reply_entry(&e);
}

void rpc_task::run_lookup_blob() {

    const fuse_ino_t parent_ino = this->rpc_api->lookup_task.get_parent_ino();
    const std::string blobName = this->rpc_api->lookup_task.get_file_name();
    Azure::Storage::Blobs::BlobContainerClient contClient = this->get_client()->get_container_client();

    bool async_task_set = set_async_function([parent_ino, blobName, contClient](rpc_task* task) {
        AZLogInfo("We are in run_lookup_blob for inode: {} and blob: {}", parent_ino, blobName);

        try {

            // const std::string containerName = "testcont";

            // auto blobClient = Azure::Storage::Blobs::BlockBlobClient::CreateFromConnectionString(
            //     connectionString, containerName, blobName);

            auto blobClient = contClient.GetBlockBlobClient(blobName);

            auto propertiesResponse = blobClient.GetProperties();
            auto blobProperties = std::make_unique<Azure::Storage::Blobs::Models::BlobProperties>(
                propertiesResponse.Value);

            task->rpc_api->lookup_task.set_parent_ino(parent_ino);
            task->rpc_api->lookup_task.set_file_name(blobName.c_str());

            lookup_callback(nullptr, 0, blobProperties.get(), task);

        } catch (const std::exception& e) {
            AZLogError("Error in lookup task: {}", e.what());
                task->reply_error(EIO);  // General input/output error
        }
    });

    if (!async_task_set) {
        AZLogError("Failed to set async lookup task.");
    }
}



void rpc_task::run_lookup()
{
    AZLogInfo("run_lookup called");
    run_lookup_blob();
    //fuse_ino_t parent_ino = rpc_api->lookup_task.get_parent_ino();

//     struct nfs_inode *inode = get_client()->get_nfs_inode_from_ino(parent_ino);
//     bool rpc_retry;
//     const char *const filename = (char*) rpc_api->lookup_task.get_file_name();

//     INC_GBL_STATS(tot_lookup_reqs, 1);

//     /*
//         * Lookup dnlc to see if we have valid cached lookup data.
//         */
//     if (aznfsc_cfg.cache.attr.user.enable) {
//         bool negative_confirmed = false;
//         struct nfs_inode *child_inode =
//             inode->dnlc_lookup(filename, &negative_confirmed);
//         if (child_inode) {
//             AZLogDebug("[{}/{}] Returning cached lookup, child_ino={}",
//                     parent_ino, filename, child_inode->get_fuse_ino());

//             INC_GBL_STATS(lookup_served_from_cache, 1);

//             struct fattr3 fattr;
//             child_inode->fattr3_from_stat(fattr);
//             get_client()->reply_entry(
//                 this,
//                 &child_inode->get_fh(),
//                 &fattr,
//                 rpc_api->lookup_task.get_fuse_file());

//             // Drop the ref held by dnlc_lookup().
//             child_inode->decref();
//             return;
//         } else if (negative_confirmed &&
//             (get_proxy_op_type() == FUSE_LOOKUP)) {
//             AZLogDebug("[{}/{}] Returning cached lookup (negative)",
//                     parent_ino, filename);

//             INC_GBL_STATS(lookup_served_from_cache, 1);
//             get_client()->reply_entry(this,
//                     nullptr /* fh */,
//                     nullptr /* fattr */,
//                     nullptr /* file */);
//             return;
//         }
//     }

//     do {
//         LOOKUP3args args;
//         args.what.dir = inode->get_fh();
//         args.what.name = (char *) filename;

//         rpc_retry = false;
//         /*
//             * Note: Once we call the libnfs async method, the callback can get
//             *       called anytime after that, even before it returns to the
//             *       caller. Since callback can free the task, it's not safe to
//             *       access the task object after making the libnfs call.
//             */
//         stats.on_rpc_issue();
//         if (rpc_nfs3_lookup_task(get_rpc_ctx(), lookup_callback, &args,
//                                     this) == NULL) {
//             stats.on_rpc_cancel();
//             /*
//                 * Most common reason for this is memory allocation failure,
//                 * hence wait for some time before retrying. Also block the
//                 * current thread as we really want to slow down things.
//                 *
//                 * TODO: For soft mount should we fail this?
//                 */
//             rpc_retry = true;

//             AZLogWarn("rpc_nfs3_lookup_task failed to issue, retrying "
//                         "after 5 secs!");
//             ::sleep(5);
//         }
//     } while (rpc_retry);
}

void rpc_task::run_access()
{
    AZLogInfo("run_access called");
}

void rpc_task::run_write()
{
    AZLogInfo("run_write called");
}

void rpc_task::run_flush()
{
    const fuse_ino_t ino = rpc_api->flush_task.get_ino();
    struct blob_inode *const inode = get_client()->get_blob_inode_from_ino(ino).get();

    /*
     * Flush must always be done in a fuse request context, as it may have
     * to wait. Waiting in libnfs thread context will cause deadlocks.
     */
    assert(get_fuse_req() != nullptr);

    reply_error(inode->flush_cache_and_wait());
}


void rpc_task::run_statfs()
{
    AZLogInfo("run_statfs called");
}

void rpc_task::run_create_file()
{
    AZLogInfo("run_create_file called");
}

void rpc_task::run_mknod()
{
    AZLogInfo("run_mknod called");
}

void rpc_task::run_mkdir()
{
    AZLogInfo("run_mkdir called");
}

void rpc_task::run_unlink()
{
    AZLogInfo("run_unlink called");
}

void rpc_task::run_rmdir()
{
    AZLogInfo("run_rmdir called");
}

void rpc_task::run_symlink()
{
    AZLogInfo("run_symlink called");
}

void rpc_task::run_rename()
{
    AZLogInfo("run_rename called");
}

void rpc_task::run_readlink()
{
    AZLogInfo("run_readlink called");
}

void rpc_task::run_setattr()
{
    AZLogInfo("run_setattr called");
}

void rpc_task::run_read()
{
    //const fuse_ino_t ino = rpc_api->read_task.get_ino();
    //struct blob_inode *inode = get_client()->get_blob_inode_from_ino(ino).get();
    // struct nfs_inode *inode = get_client()->get_nfs_inode_from_ino(ino);
    // int64_t cfsize, sfsize;

    // run_read() must be called only for an fe task.
    //assert(rpc_api->read_task.is_fe());

    // assert(inode->is_regfile());
    // /*
    //  * aznfsc_ll_read() can only be called after aznfsc_ll_open() so filecache
    //  * and readahead state must have been allocated when we reach here.
    //  */
    // assert(inode->has_filecache());
    // assert(inode->has_rastate());

    // std::shared_ptr<bytes_chunk_cache>& filecache_handle =
    //     inode->get_filecache();

    // /*
    //  * run_read() is called once for a fuse read request and must not be
    //  * called for a child task.
    //  */
    // assert(rpc_api->parent_task == nullptr);

    // /*
    //  * Get server and effective file size estimates.
    //  * We use these to find holes that we should zero fill.
    //  */
    // inode->get_file_sizes(cfsize, sfsize);

    // /*
    //  * get_client_file_size() returns a fairly recent estimate of the file
    //  * size taking into account cto consistency, attribute cache timeout, etc,
    //  * so it's a size we can "trust". If READ offset is more than this size
    //  * then we can avoid sending the call to the server and generate eof
    //  * locally. This is an optimization that saves an extra READ call to the
    //  * server. The server will correctly return 0+eof for this READ call so we
    //  * are functionally correct, barring the small possibility that the file
    //  * can grow and if we send the READ to the server we will get the new data,
    //  * but this is permissible with our weak/cto cache consistency guarantees.
    //  *
    //  * TODO: Shall we put it behind a config?
    //  */
    // if (cfsize != -1) {
    //     if (rpc_api->read_task.get_offset() >= cfsize) {
    //         /*
    //          * Entire request lies beyond cfsize.
    //          */
    //         AZLogDebug("[{}] Read returning 0 bytes (eof) as requested "
    //                    "offset ({}) >= file size ({})",
    //                    ino, rpc_api->read_task.get_offset(), cfsize);
    //         INC_GBL_STATS(zero_reads, 1);
    //         reply_iov(nullptr, 0);
    //         return;
    //     } else if ((rpc_api->read_task.get_offset() +
    //                 rpc_api->read_task.get_size()) > (uint64_t) cfsize) {
    //         /*
    //          * Request extends beyond cfsize, trim it so that we don't have
    //          * to worry about excluding it later.
    //          */
    //         const uint64_t trim_len =
    //             (rpc_api->read_task.get_offset() +
    //              rpc_api->read_task.get_size()) - cfsize;
    //         const uint64_t trimmed_size =
    //             rpc_api->read_task.get_size() - trim_len;
    //         assert(trimmed_size < rpc_api->read_task.get_size());

    //         AZLogDebug("[{}] Trimming application read beyond cfsize: {} "
    //                    "({} -> {})",
    //                    ino, cfsize, rpc_api->read_task.get_size(), trimmed_size);

    //         rpc_api->read_task.set_size(trimmed_size);
    //     }
    // }

    // /*
    //  * Get bytes_chunks covering the region caller wants to read.
    //  * The bytes_chunks returned could be any mix of old (already cached) or
    //  * new (cache allocated but yet to be read from blob). Note that reads
    //  * don't need any special protection. The caller just wants to read the
    //  * current contents of the blob, these can change immediately after or
    //  * even while we are reading, resulting in any mix of old or new data.
    //  */
    // assert(bc_vec.empty());
    // bc_vec = filecache_handle->get(
    //                    rpc_api->read_task.get_offset(),
    //                    rpc_api->read_task.get_size());

    // /*
    //  * inode->in_ra_window() (called from inline_prune()) considers anything
    //  * before max_byte_read as non-useful and can purge that from the cache.
    //  * Now that we have called get() we will have an inuse count on any
    //  * membuf(s) used by this read, so they won't be purged even if
    //  * in_ra_window() suggests that, so we can safely update max_byte_read now.
    //  * We don't want to wait for read_callback() to update max_byte_read, as
    //  * that would mean many readers may be waiting to be read and we won't
    //  * perform sufficient readahead.
    //  */
    // inode->get_rastate()->on_application_read(
    //         rpc_api->read_task.get_offset(),
    //         rpc_api->read_task.get_size());

    // /*
    //  * send_read_response() will later convey this read completion to fuse
    //  * using fuse_reply_iov() which can send max FUSE_REPLY_IOV_MAX_COUNT
    //  * vector elements.
    //  */
    // const size_t size = std::min((int) bc_vec.size(), FUSE_REPLY_IOV_MAX_COUNT);
    // assert(size > 0 && size <= FUSE_REPLY_IOV_MAX_COUNT);

    // // There should not be any reads running for this RPC task initially.
    // assert(num_ongoing_backend_reads == 0);

    // AZLogDebug("[{}] run_read: offset {}, size: {}, chunks: {}{}, "
    //            "sfsize: {}, cfsize: {}, csfsize: {}",
    //            ino,
    //            rpc_api->read_task.get_offset(),
    //            rpc_api->read_task.get_size(),
    //            bc_vec.size(),
    //            size != bc_vec.size() ? " (capped at 1023)" : "",
    //            sfsize, cfsize, inode->get_cached_filesize());

    run_read_blob(rpc_api->read_task.get_size(), rpc_api->read_task.get_offset());


    /*
     * Now go through the byte chunk vector to see if the chunks are
     * uptodate. Uptodate chunks already have the data we need, while non
     * uptodate chunks need to be populated with data. These will mostly be
     * READ from the server, with certain exceptions. See below.
     * Chunks that are READ from the server are issued in parallel. Once all
     * chunks are uptodate we can complete the read to the caller.
     *
     * Here are the rules that we follow to decide if a non-uptodate chunk
     * must be READ from the server.
     * - (bc.offset + bc.length) > get_client_file_size()
     *   These MUST NOT occur as we trim the read request before making the
     *   bytes_chunk_cache::get() call. We assert for these.
     * - bc.offset < get_server_file_size()
     *   Part or whole of the bc need to be read from the server, so we MUST
     *   issue READ to the server for such bcs. If part of the bc lies after
     *   the server file size, read_callback() will fill 0s for that part.
     *   This is correct as that part corresponds to hole in our cache.
     * - bc.offset >= get_server_file_size()
     *   Server does not know about these so we MUST NOT ask the server about
     *   these. Given that the bc is non-uptodate this means our cache also
     *   doesn't have it, which means these correspond to holes in our cache
     *   and we MUST return 0s for these bcs.
     *
     * Note that we bump num_ongoing_backend_reads by 1 before issuing
     * the first backend read. This is done to make sure if read_callback()
     * is called before we could issues all reads, we don't mistake it for
     * "all issued reads have completed". It is ok to update this without a lock
     * since this is the only thread at this point which will access this.
     *
     * Note: Membufs which are found uptodate here shouldn't suddenly become
     *       non-uptodate when the other reads complete, o/w we have a problem.
     *       An uptodate membuf doesn't become non-uptodate but it can be
     *       written by some writer thread, while we are waiting for other
     *       chunk(s) to be read from backend or even while we are reading
     *       them while sending the READ response.
     */

// #ifdef RELEASE_CHUNK_AFTER_APPLICATION_READ
//                 /*
//                  * Since the data is read from the cache, the chances of reading
//                  * it again from cache is negligible since this is a sequential
//                  * read pattern.
//                  * Free such chunks to reduce the memory utilization.
//                  */
//                 //filecache_handle->release(bc_vec[i].offset, bc_vec[i].length);
// #endif

            /*
             * Ok, non-uptodate buffer, see if we should read from server or
             * return 0s. We read from server only if at least one byte from
             * the bc can be served from the server, else it's the case of
             * unmapped chunk within the cached portion (case of sparse cache
             * due to sparse writes beyond file size) which should correspond
             * with holes aka 0s.
             */
//             const bool read_from_server =
//                 ((sfsize == -1) || (int64_t) bc_vec[i].offset < sfsize);

//             if (!read_from_server) {
//                 AZLogDebug("[{}] Hole in cache. offset: {}, length: {}",
//                            ino, bc_vec[i].offset, bc_vec[i].length);

//                 /*
//                  * Set "bytes read" to "bytes requested" since we provide all
//                  * the data as 0s.
//                  */
//                 bc_vec[i].pvt = bc_vec[i].length;
//                 ::memset(bc_vec[i].get_buffer(), 0, bc_vec[i].length);

//                 bc_vec[i].get_membuf()->set_uptodate();
//                 bc_vec[i].get_membuf()->clear_locked();
//                 bc_vec[i].get_membuf()->clear_inuse();

//                 INC_GBL_STATS(bytes_zeroed_from_cache, bc_vec[i].length);
// #ifdef RELEASE_CHUNK_AFTER_APPLICATION_READ
//                 filecache_handle->release(bc_vec[i].offset, bc_vec[i].length);
// #endif
//                 continue;
//             }

            //found_in_cache = false;

            /*
             * TODO: If we have just 1 bytes_chunk to fill, which is the most
             *       common case, avoid creating child task and process
             *       everything in this same task.
             *       Also for contiguous reads use the libnfs vectored read API.
             */

            /*
             * Create a child rpc task to issue the read RPC to the backend.
             */
            struct rpc_task *child_tsk =
                get_client()->get_rpc_task_helper()->alloc_rpc_task_reserved(FUSE_READ);

            // child_tsk->init_read_be(
            //     rpc_api->read_task.get_ino(),
            //     bc_vec[i].length,
            //     bc_vec[i].offset);

            // Set the parent task of the child to the current RPC task.
            child_tsk->rpc_api->parent_task = this;

            /*
             * Set "bytes read" to 0 and this will be updated as data is read,
             * likely in partial read calls. So at any time bc.pvt will be the
             * total data read.
             */

            /*
             * Child task should always read a subset of the parent task.
             */
            // assert(child_tsk->rpc_api->read_task.get_offset() >=
            //        rpc_api->read_task.get_offset());
            // assert(child_tsk->rpc_api->read_task.get_size() <=
            //        rpc_api->read_task.get_size());



    // get() must return bytes_chunks exactly covering the requested range.
    //assert(total_length == rpc_api->read_task.get_size());
    AZLogDebug("Size: {}", rpc_api->read_task.get_size());
    // send_read_response();
    return;
}

void rpc_task::send_read_response()
{
    // This should always be called on the parent task.
    assert(rpc_api->parent_task == nullptr);

    [[maybe_unused]] const fuse_ino_t ino = rpc_api->read_task.get_ino();

    /*
     * We must send response only after all component reads complete, they may
     * succeed or fail.
     */
    assert(num_ongoing_backend_reads == 0);

    if (read_status != 0) {
        INC_GBL_STATS(failed_read_reqs, 1);
        // Non-zero status indicates failure, reply with error in such cases.
        AZLogDebug("[{}] Sending failed read response {}", ino, read_status.load());

        reply_error(read_status);
        return;
    }

    /*
     * Now go over all the chunks and send a vectored read response to fuse.
     * Note that only the last chunk can be partial.
     * XXX No, in case of multiple chunks and short read, multiple can be
     *     partial.
     */
    size_t count = bc_vec.size();

    // Create an array of iovec struct
    struct iovec iov[count];
    uint64_t bytes_read = 0;
    [[maybe_unused]] bool partial_read = false;

    for (size_t i = 0; i < count; i++) {
        assert(bc_vec[i].pvt <= bc_vec[i].length);

        iov[i].iov_base = (void *) bc_vec[i].get_buffer();
        iov[i].iov_len = bc_vec[i].pvt;

        bytes_read += bc_vec[i].pvt;

        if (bc_vec[i].pvt < bc_vec[i].length) {
#if 0
            assert((i == count-1) || (bc_vec[i+1].length == 0));
#endif
            partial_read = true;
            count = i + 1;
            break;
        }
    }
    AZLogDebug("Bytes read:" , bytes_read);

    /*
     * If bc_vec.size() == FUSE_REPLY_IOV_MAX_COUNT, then it may have been
     * trimmed, so we cannot assert for bytes_read to exactly match requested
     * size.
     */
    // assert((bytes_read == rpc_api->read_task.get_size()) ||
    //        partial_read || (bc_vec.size() == FUSE_REPLY_IOV_MAX_COUNT));

    /*
     * Currently fuse sends max 1MiB read requests, so we should never
     * be responding more than that.
     * This is a sanity assert for catching unintended bugs, update if
     * fuse max read size changes.
     */
    //assert(bytes_read <= 1048576);

    // Send response to caller.
    if (bytes_read == 0) {
        INC_GBL_STATS(zero_reads, 1);
        AZLogDebug("[{}] Sending empty read response", ino);
        reply_iov(nullptr, 0);
    } else {
        INC_GBL_STATS(app_bytes_read, bytes_read);
        AZLogDebug("[{}] Sending success read response, iovec={}, "
                   "bytes_read={}",
                   ino, count, bytes_read);
        reply_iov(iov, count);
    }
}

struct read_context
{
    rpc_task *task;
    struct bytes_chunk *bc;

    read_context(
        rpc_task *_task,
        struct bytes_chunk *_bc):
        task(_task),
        bc(_bc)
    {
        assert(task->magic == RPC_TASK_MAGIC);
        // assert(bc->length > 0 && bc->length <= AZNFSC_MAX_CHUNK_SIZE);
        // assert(bc->offset < AZNFSC_MAX_FILE_SIZE);
    }
};



static void read_callback_blob(
    struct rpc_context *rpc,
    int rpc_status,
    void *data,
    void *private_data)
{
    struct read_context *ctx = static_cast<read_context*>(private_data);
    rpc_task *task = ctx->task;
    assert(task->magic == RPC_TASK_MAGIC);

    auto buffer = static_cast<std::vector<uint8_t>*>(data);
    if (!buffer || buffer->empty()) {
        AZLogError("Error: Read buffer is NULL or empty for inode {}", task->rpc_api->read_task.get_ino());
        task->reply_error(ENOENT);
        delete buffer;
        delete ctx;
        return;
    }

    size_t size = buffer->size();
    AZLogDebug("Blob read completed for inode {}, size {}", task->rpc_api->read_task.get_ino(), size);

    struct iovec iov[1];
    iov[0].iov_base = buffer->data();
    iov[0].iov_len = size;

    if (size == 0) {
        INC_GBL_STATS(zero_reads, 1);
        AZLogDebug("[{}] Sending empty read response", task->rpc_api->read_task.get_ino());
        task->reply_iov(nullptr, 0);
    } else {
        INC_GBL_STATS(app_bytes_read, size);
        AZLogDebug("[{}] Sending success read response, iovec=1, bytes_read={}",
                   task->rpc_api->read_task.get_ino(), size);
        task->reply_iov(iov, 1);
    }

    // Clean up
    delete buffer;
    delete ctx;
}



void rpc_task::run_read_blob(size_t size, off_t offset) {
    Azure::Storage::Blobs::BlobContainerClient contClient = this->get_client()->get_container_client();

    bool async_task_set = set_async_function([size, offset, contClient](rpc_task *task) {
        try {
            const fuse_ino_t ino = task->rpc_api->read_task.get_ino();
            std::shared_ptr<blob_inode> inode = task->get_client()->get_blob_inode_from_ino(ino);
            

            if (!inode) {
                AZLogError("Error: blob_inode is NULL for inode {}", ino);
                task->reply_error(ENOENT);
                return;
            }

            auto blobClient = contClient.GetBlockBlobClient(inode->blob_name);

            AZLogDebug("Reading {} bytes from blob '{}', offset {}", size, inode->blob_name, offset);

            Azure::Storage::Blobs::DownloadBlobOptions options;
            options.Range = Azure::Core::Http::HttpRange();
            options.Range.Value().Offset = offset;
            options.Range.Value().Length = size;

            auto response = blobClient.Download(options);

            auto bodyStream = response.Value.BodyStream.get();
            auto buffer = new std::vector<uint8_t>(bodyStream->Length());
            bodyStream->ReadToCount(buffer->data(), buffer->size());

            struct read_context *ctx = new read_context(task, nullptr);

            // Call the callback
            read_callback_blob(nullptr, 0, buffer, ctx);

        } catch (const std::exception &e) {
            AZLogError("Error in async read task: {}", e.what());
            task->reply_error(EIO);
        }
    });

    if (!async_task_set) {
        AZLogError("Failed to set async read task.");
    }
}


void rpc_task::run_readdir()
{
    AZLogInfo("run_readdir called");
}

void rpc_task::run_readdirplus()
{
    AZLogInfo("run_readdirplus called");
}

void rpc_task::free_rpc_task()
{
    assert(get_op_type() <= FUSE_OPCODE_MAX);

    // We shouldn't be freeing any task still not complete.
    assert(num_ongoing_backend_writes == 0);
    assert(num_ongoing_backend_reads == 0);

    /*
     * Destruct anything allocated by the RPC API specific structs.
     * The rpc_api itself continues to exist. We avoid alloc/dealloc for every
     * task.
     */
    if (rpc_api) {
        rpc_api->release();
    }

    /*
     * Some RPCs store some data in the rpc_task directly.
     * That can be cleaned up here.
     */
    switch(get_op_type()) {
    case FUSE_READ:
    {

        /*
         * rpc_api->parent_task will be nullptr for a parent task.
         * Only parent tasks run the fuse request so only they can have
         * bc_vec[] non-empty. Also only parent tasks can have read_status as
         * non-zero as the overall status of the fuse read is tracked by the
         * parent task.
         * Also, since only child tasks send the actual READ RPC, only they
         * can fail with jukebox error and hence only they can have rpc_api
         * as nullptr.
         *
         * Note: bc_vec.empty() => child task.
         *       (read_status != 0) => parent_task
         */
        [[maybe_unused]] const bool is_parent_task =
            (rpc_api && rpc_api->parent_task == nullptr);
        assert(bc_vec.empty() || is_parent_task);
        assert((read_status == 0) || is_parent_task);
        assert(num_ongoing_backend_reads == 0);

        read_status = 0;
        bc_vec.clear();
        break;
    }
    default :
        break;
    }

    stats.on_rpc_free();
    client->get_rpc_task_helper()->free_rpc_task(this);
    AZLogInfo("Task freed successfully");
}

void rpc_task::issue_commit_rpc()
{
    AZLogInfo("issue_commit_rpc called");
}

bool rpc_task::add_bc(const bytes_chunk& bc)
{
    AZLogInfo("add_bc called");
    return true;
}

void rpc_task::init_getattr(fuse_req *request,
                            fuse_ino_t ino)
{
    assert(get_op_type() == FUSE_GETATTR);
    set_fuse_req(request);
    rpc_api->getattr_task.set_ino(ino);
    AZLogInfo(" INIT {}", rpc_api->getattr_task.get_ino());

    //fh_hash = get_client()->get_nfs_inode_from_ino(ino)->get_crc();
}

void rpc_task::do_proxy_getattr() const
{
    // struct nfs_inode *inode =
    //     get_client()->get_nfs_inode_from_ino(rpc_api->get_ino());

    // /*
    //  * Caller calls us to fetch attributes from the server and not return
    //  * the cached attributes, force attributes to be fetched from the server.
    //  */
    // inode->invalidate_attribute_cache();

    AZLogWarn("Sending proxy getattr on behalf of {}: req: {}, ino: {}",
              rpc_task::fuse_opcode_to_string(get_op_type()),
              fmt::ptr(get_fuse_req()),
              rpc_api->get_ino());

    /*
     * Currently only called for SETATTR.
     */
    assert(get_op_type() == FUSE_SETATTR);

    struct rpc_task *proxy_task =
        get_client()->get_rpc_task_helper()->alloc_rpc_task_reserved(FUSE_GETATTR);

    proxy_task->init_getattr(get_fuse_req(),
                             rpc_api->get_ino());
    proxy_task->set_proxy_op_type(get_op_type());

    proxy_task->run_getattr();
}

void rpc_task::issue_write_rpc()
{
    AZLogInfo("issue_write_rpc called");
}


void rpc_task::read_from_server(struct bytes_chunk &bc)
{
    AZLogInfo("read_from_server called");
}

void stat_from_blob_properties(struct stat& st, 
        const Azure::Storage::Blobs::Models::BlobProperties& properties, 
        const fuse_ino_t ino,
        rpc_task *task) {
    ::memset(&st, 0, sizeof(st));

    st.st_ino = ino;
    if (ino == 1)
    {
        st.st_mode = S_IFDIR | 0644;
    }
    else
    {
        st.st_mode = S_IFREG | 0644; // Ensure regular file mode with proper permissions
    }
    st.st_nlink = 1; // Number of hard links
    // st.st_uid = 1000; // Set to the current user ID
    // st.st_gid = 1000; // Set to the current group ID
    st.st_size = properties.BlobSize; // Blob size
    st.st_blksize = 4096; // Block size
    st.st_blocks = properties.BlobSize/4096 ; // Number of 512-byte blocks

    // Convert LastModified to time_t and populate timestamps
    auto last_modified_time = std::chrono::duration_cast<std::chrono::seconds>(
                            properties.LastModified.time_since_epoch()).count();
    st.st_mtim.tv_sec = last_modified_time;
    st.st_mtim.tv_nsec = 0;
    st.st_ctim = st.st_mtim; // Set creation time to match modification time
    st.st_atim = st.st_mtim; // Set access time to match modification time
}


static void getattr_callback_blob(
    struct rpc_context* rpc,
    int rpc_status,
    void* data,
    void* private_data) {

    rpc_task* task = static_cast<rpc_task*>(private_data);
    assert(task->magic == RPC_TASK_MAGIC);

    const fuse_ino_t ino = task->rpc_api->getattr_task.get_ino();
    
    auto* blobProperties = static_cast<Azure::Storage::Blobs::Models::BlobProperties*>(data);
    if (!blobProperties) {
        AZLogError("getattr_callback_blob: blobProperties is NULL for inode {}", ino);
        task->reply_error(EIO);
        return;
    }

    AZLogDebug("getattr_callback_blob: Blob properties fetched for inode {}", ino);
    AZLogDebug("  - Blob Name: {}", "newfile.log");
    AZLogDebug("  - Size: {} bytes", blobProperties->BlobSize);
    AZLogDebug("  - Last Modified: {}", blobProperties->LastModified.ToString());
    AZLogDebug("  - ETag: {}", blobProperties->ETag.ToString());

    struct stat st;
    stat_from_blob_properties(st, *blobProperties, ino, task);

    AZLogDebug("getattr_callback_blob: Final stat for inode {} - Size: {}, Mode: {}, MTime: {}", 
               ino, st.st_size, st.st_mode, st.st_mtim.tv_sec);

    task->reply_attr(st, 30);
}

void rpc_task::run_getattr_blob() {
    const std::string blobName = "newfile.log";
    Azure::Storage::Blobs::BlobContainerClient contClient = this->get_client()->get_container_client();

    bool async_task_set = set_async_function([contClient, blobName](rpc_task* task) {
        AZLogInfo("We are in run_getattr_blob");
        try {
            auto blobClient = contClient.GetBlockBlobClient(blobName);

            // Fetch blob properties synchronously
            auto propertiesResponse = blobClient.GetProperties();
            auto blobProperties = std::make_unique<Azure::Storage::Blobs::Models::BlobProperties>(propertiesResponse.Value);

            // Call the callback
            getattr_callback_blob(nullptr, 0, blobProperties.get(), task);

        } catch (const std::exception& e) {
            AZLogError("Error in getattr task: {}", e.what());
            task->reply_error(EIO);
        }
    });

    if (!async_task_set) {
        AZLogError("Failed to set async getattr task.");
    }
}

// static void getattr_callback(
//     struct rpc_context *rpc,
//     int rpc_status,
//     void *data,
//     void *private_data)
// {
//     rpc_task *task = (rpc_task*) private_data;
//     assert(task->magic == RPC_TASK_MAGIC);

//     auto res = (GETATTR3res*)data;

//     INJECT_JUKEBOX(res, task);

//     const fuse_ino_t ino =
//         task->rpc_api->getattr_task.get_ino();
//     struct nfs_inode *inode =
//         task->get_client()->get_nfs_inode_from_ino(ino);
//     const int status = task->status(rpc_status, NFS_STATUS(res));

//     /*
//      * Now that the request has completed, we can query libnfs for the
//      * dispatch time.
//      */
//     task->get_stats().on_rpc_complete(rpc_get_pdu(rpc), NFS_STATUSX(rpc_status, res));

//     if (status == 0) {
//         // Got fresh attributes, update the attributes cached in the inode.
//         inode->update(&(res->GETATTR3res_u.resok.obj_attributes));

//         /*
//          * Set fuse kernel attribute cache timeout to the current attribute
//          * cache timeout for this inode, as per the recent revalidation
//          * experience.
//          */
//         task->reply_attr(inode->get_attr(), inode->get_actimeo());
//     } else if (NFS_STATUS(res) == NFS3ERR_JUKEBOX) {
//         task->get_client()->jukebox_retry(task);
//     } else {
//         task->reply_error(status);
//     }
// }


void rpc_task::run_getattr()
{
    bool f = true;
    if (f)
    {
        run_getattr_blob();
        return;
    }

    // bool rpc_retry;
    // auto ino = rpc_api->getattr_task.get_ino();
    // struct nfs_inode *inode = get_client()->get_nfs_inode_from_ino(ino);

    // INC_GBL_STATS(tot_getattr_reqs, 1);

    // /*
    //  * If inode's cached attribute is valid, use that.
    //  */
    // if (aznfsc_cfg.cache.attr.user.enable) {
    //     if (!inode->attr_cache_expired()) {
    //         INC_GBL_STATS(getattr_served_from_cache, 1);
    //         AZLogDebug("[{}] Returning cached attributes", ino);
    //         reply_attr(inode->get_attr(), inode->get_actimeo());
    //         return;
    //     }
    // }

    // do {
    //     GETATTR3args args;

    //     args.object = inode->get_fh();

    //     rpc_retry = false;
    //     stats.on_rpc_issue();
    //     if (rpc_nfs3_getattr_task(get_rpc_ctx(), getattr_callback, &args,
    //                               this) == NULL) {
    //         stats.on_rpc_cancel();
    //         /*
    //          * Most common reason for this is memory allocation failure,
    //          * hence wait for some time before retrying. Also block the
    //          * current thread as we really want to slow down things.
    //          *
    //          * TODO: For soft mount should we fail this?
    //          */
    //         rpc_retry = true;

    //         AZLogWarn("rpc_nfs3_getattr_task failed to issue, retrying "
    //                   "after 5 secs!");
    //         ::sleep(5);
    //     }
    // } while (rpc_retry);
        
}

void rpc_task::get_readdir_entries_from_cache()
{
    AZLogInfo("get_readdir_entries_from_cache called");
}

void rpc_task::fetch_readdir_entries_from_server()
{
    AZLogInfo("fetch_readdir_entries_from_server called");
}

void rpc_task::fetch_readdirplus_entries_from_server()
{
    AZLogInfo("fetch_readdirplus_entries_from_server called");
}

void rpc_task::send_readdir_or_readdirplus_response(
    const std::vector<std::shared_ptr<const directory_entry>>& readdirentries)
{
    AZLogInfo("send_readdir_or_readdirplus_response called");
}
