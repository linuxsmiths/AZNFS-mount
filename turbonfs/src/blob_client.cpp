#include "blob_client.h"
#include "aznfsc.h"
#include "nfs_internal.h"
#include "rpc_task.h"
#include "rpc_readdir.h"


/*
 * This function will be called only to retry the commit requests that failed
 * with JUKEBOX error.
 * rpc_api defines the RPC request that need to be retried.
 */
void blob_client::jukebox_flush(struct api_task_info *rpc_api)
{
    /*
     * For commit task pvt has bc_vec, which has copy of byte_chunk vector.
     * To proceed it should be valid.
     *
     * Note: Commit task is always a backend task, 'req' is nullptr.
     */
    assert(rpc_api->pvt != nullptr);
    assert(rpc_api->optype == FUSE_FLUSH);
    assert(rpc_api->req == nullptr);

    /*
     * Create a new FUSE_FLUSH task to retry the commit request.
     */
    struct rpc_task *commit_task =
        get_rpc_task_helper()->alloc_rpc_task(FUSE_FLUSH);
    commit_task->init_flush(nullptr /* fuse_req */,
                            rpc_api->flush_task.get_ino());
    commit_task->rpc_api->pvt = rpc_api->pvt;
    rpc_api->pvt = nullptr;

    // Any new task should start fresh as a parent task.
    assert(commit_task->rpc_api->parent_task == nullptr);

    commit_task->issue_commit_rpc();
}

/*
 * This function will be called only to retry the write requests that failed
 * with JUKEBOX error.
 * rpc_api defines the RPC request that need to be retried.
 */
void blob_client::jukebox_write(struct api_task_info *rpc_api)
{
    // Only BE tasks can be retried.
    assert(rpc_api->write_task.is_be());

    /*
     * For write task pvt has write_iov_context, which has copy of byte_chunk vector.
     * To proceed it should be valid.
     */
    assert(rpc_api->pvt != nullptr);
    assert(rpc_api->optype == FUSE_WRITE);

    struct rpc_task *write_task =
        get_rpc_task_helper()->alloc_rpc_task(FUSE_WRITE);
    write_task->init_write_be(rpc_api->write_task.get_ino());

    // Any new task should start fresh as a parent task.
    assert(write_task->rpc_api->parent_task == nullptr);

    [[maybe_unused]] struct bc_iovec *bciov = (struct bc_iovec *) rpc_api->pvt;
    assert(bciov->magic == BC_IOVEC_MAGIC);

    // TODO: Make this a unique_ptr?
    write_task->rpc_api->pvt = rpc_api->pvt;
    rpc_api->pvt = nullptr;

#if 0
    /*
     * We currently only support buffered writes where the original fuse write
     * task completes after copying data to the bytes_chunk_cache and later
     * we sync the dirty membuf using one or more flush rpc_tasks whose sole
     * job is to ensure they sync the part of the blob they are assigned.
     * They don't need a parent_task which is usually the fuse task that needs
     * to be completed once the underlying tasks complete.
     *
     * Note: This is no longer true, see parent_task argument to sync_membufs().
     */
    assert(rpc_api->parent_task == nullptr);
#endif

    /*
     * If the write task that failed with jukebox is a child of a fuse write
     * task, then we have to complete the parent write task when the child
     * and all children complete. We need to set the parent_task of the retried
     * task too.
     */
    if (rpc_api->parent_task != nullptr) {
        assert(rpc_api->parent_task->magic == RPC_TASK_MAGIC);
        assert(rpc_api->parent_task->get_op_type() == FUSE_WRITE);
        assert(rpc_api->parent_task->rpc_api->write_task.is_fe());
        // At least this child task has not completed.
        assert(rpc_api->parent_task->num_ongoing_backend_writes > 0);

        write_task->rpc_api->parent_task = rpc_api->parent_task;
    }

    write_task->issue_write_rpc();
}

/*
 * This function will be called only to retry the read requests that failed
 * with JUKEBOX error.
 * rpc_api defines the RPC request that need to be retried.
 */
void blob_client::jukebox_read(struct api_task_info *rpc_api)
{
    assert(rpc_api->optype == FUSE_READ);

    struct rpc_task *child_tsk =
        get_rpc_task_helper()->alloc_rpc_task(FUSE_READ);

    child_tsk->init_read_be(
        rpc_api->read_task.get_ino(),
        rpc_api->read_task.get_size(),
        rpc_api->read_task.get_offset());

    /*
     * Read API calls will be issued only for BE tasks, hence
     * copy the parent info from the original task to this retry task.
     */
    assert(rpc_api->parent_task != nullptr);
    assert(rpc_api->parent_task->magic == RPC_TASK_MAGIC);
    assert(rpc_api->parent_task->rpc_api->read_task.is_fe());
    // At least this child task has not completed.
    assert(rpc_api->parent_task->num_ongoing_backend_reads > 0);

    child_tsk->rpc_api->parent_task = rpc_api->parent_task;

    [[maybe_unused]]  const struct rpc_task *const parent_task =
        child_tsk->rpc_api->parent_task;

    /*
     * Since we are retrying this child task, the parent read task should have
     * atleast 1 ongoing read.
     */
    assert(parent_task->num_ongoing_backend_reads > 0);

    /*
     * Child task should always read a subset of the parent task.
     */
    assert(child_tsk->rpc_api->read_task.get_offset() >=
            parent_task->rpc_api->read_task.get_offset());
    assert(child_tsk->rpc_api->read_task.get_size() <=
            parent_task->rpc_api->read_task.get_size());

    assert(rpc_api->bc != nullptr);

    // Jukebox retry is for an existing request issued to the backend.
    assert(rpc_api->bc->num_backend_calls_issued > 0);

#ifdef ENABLE_PARANOID
    {
        unsigned int i;
        for (i = 0; i < parent_task->bc_vec.size(); i++) {
            if (rpc_api->bc == &parent_task->bc_vec[i])
                break;
        }

        /*
         * rpc_api->bc MUST refer to one of the elements in
         * parent_task->bc_vec.
         */
        assert(i != parent_task->bc_vec.size());
    }
#endif

    /*
     * The jukebox retry task also should read into the same bc.
     */
    child_tsk->rpc_api->bc = rpc_api->bc;

    /*
     * The bytes_chunk held by this task must have its inuse count
     * bumped as the get() call made to obtain this chunk initially would
     * have set it.
     */
    assert(rpc_api->bc->get_membuf()->is_inuse());

    // Issue the read to the server
    child_tsk->read_from_server(*(rpc_api->bc));
}


void blob_client::jukebox_runner()
{
    AZLogDebug("Started jukebox_runner");

    do {
        int jukebox_requests;

        {
            std::unique_lock<std::mutex> lock(jukebox_seeds_lock_39);
            jukebox_requests = jukebox_seeds.size();
        }

        /*
         * If no jukebox queued, wait more else wait less in order to meet the
         * 5 sec jukebox deadline.
         */
        if (jukebox_requests == 0) {
            ::sleep(5);
        } else {
            ::sleep(1);
        }

        {
            std::unique_lock<std::mutex> lock(jukebox_seeds_lock_39);
            jukebox_requests = jukebox_seeds.size();
            if (jukebox_requests == 0) {
                continue;
            }
        }

        AZLogDebug("jukebox_runner woken up ({} requests in queue)",
                   jukebox_requests);

        /*
         * Go over all queued requests and issue those which are ready to be
         * issued, i.e., they have been queued for more than JUKEBOX_DELAY_SECS
         * seconds. We issue the requests after releasing jukebox_seeds_lock_39.
         */
        std::vector<jukebox_seedinfo *> jsv;
        {
            std::unique_lock<std::mutex> lock(jukebox_seeds_lock_39);
            while (!jukebox_seeds.empty()) {
                struct jukebox_seedinfo *js = jukebox_seeds.front();

                if (js->run_at_msecs > get_current_msecs()) {
                    break;
                }

                jukebox_seeds.pop();

                jsv.push_back(js);
            }
        }

        for (struct jukebox_seedinfo *js : jsv) {
            switch (js->rpc_api->optype) {
                case FUSE_LOOKUP:
                    AZLogWarn("[JUKEBOX REISSUE] LOOKUP(req={}, "
                              "parent_ino={}, name={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->lookup_task.get_parent_ino(),
                              js->rpc_api->lookup_task.get_file_name());
                    lookup(js->rpc_api->req,
                           js->rpc_api->lookup_task.get_parent_ino(),
                           js->rpc_api->lookup_task.get_file_name());
                    break;
                case FUSE_ACCESS:
                    AZLogWarn("[JUKEBOX REISSUE] ACCESS(req={}, "
                              "ino={}, mask=0{:03o})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->access_task.get_ino(),
                              js->rpc_api->access_task.get_mask());
                    access(js->rpc_api->req,
                           js->rpc_api->access_task.get_ino(),
                           js->rpc_api->access_task.get_mask());
                    break;
                case FUSE_GETATTR:
                    AZLogWarn("[JUKEBOX REISSUE] GETATTR(req={}, ino={}, "
                              "fi=null)",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->getattr_task.get_ino());
                    getattr(js->rpc_api->req,
                            js->rpc_api->getattr_task.get_ino(),
                            nullptr);
                    break;
                case FUSE_SETATTR:
                    AZLogWarn("[JUKEBOX REISSUE] SETATTR(req={}, ino={}, "
                              "to_set=0x{:x}, fi={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->setattr_task.get_ino(),
                              js->rpc_api->setattr_task.get_attr_flags_to_set(),
                              fmt::ptr(js->rpc_api->setattr_task.get_fuse_file()));
                    setattr(js->rpc_api->req,
                            js->rpc_api->setattr_task.get_ino(),
                            js->rpc_api->setattr_task.get_attr(),
                            js->rpc_api->setattr_task.get_attr_flags_to_set(),
                            js->rpc_api->setattr_task.get_fuse_file());
                    break;
                case FUSE_STATFS:
                    AZLogWarn("[JUKEBOX REISSUE] STATFS(req={}, ino={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->statfs_task.get_ino());
                    statfs(js->rpc_api->req,
                           js->rpc_api->statfs_task.get_ino());
                    break;
                case FUSE_CREATE:
                    AZLogWarn("[JUKEBOX REISSUE] CREATE(req={}, parent_ino={},"
                              " name={}, mode=0{:03o}, fi={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->create_task.get_parent_ino(),
                              js->rpc_api->create_task.get_file_name(),
                              js->rpc_api->create_task.get_mode(),
                              fmt::ptr(js->rpc_api->create_task.get_fuse_file()));
                    create(js->rpc_api->req,
                           js->rpc_api->create_task.get_parent_ino(),
                           js->rpc_api->create_task.get_file_name(),
                           js->rpc_api->create_task.get_mode(),
                           js->rpc_api->create_task.get_fuse_file());
                    break;
                case FUSE_MKNOD:
                    AZLogWarn("[JUKEBOX REISSUE] MKNOD(req={}, parent_ino={},"
                              " name={}, mode=0{:03o})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->mknod_task.get_parent_ino(),
                              js->rpc_api->mknod_task.get_file_name(),
                              js->rpc_api->mknod_task.get_mode());
                    mknod(js->rpc_api->req,
                           js->rpc_api->mknod_task.get_parent_ino(),
                           js->rpc_api->mknod_task.get_file_name(),
                           js->rpc_api->mknod_task.get_mode());
                    break;
                case FUSE_MKDIR:
                    AZLogWarn("[JUKEBOX REISSUE] MKDIR(req={}, parent_ino={}, "
                              "name={}, mode=0{:03o})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->mkdir_task.get_parent_ino(),
                              js->rpc_api->mkdir_task.get_dir_name(),
                              js->rpc_api->mkdir_task.get_mode());
                    mkdir(js->rpc_api->req,
                          js->rpc_api->mkdir_task.get_parent_ino(),
                          js->rpc_api->mkdir_task.get_dir_name(),
                          js->rpc_api->mkdir_task.get_mode());
                    break;
                case FUSE_RMDIR:
                    AZLogWarn("[JUKEBOX REISSUE] RMDIR(req={}, parent_ino={}, "
                              "name={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->rmdir_task.get_parent_ino(),
                              js->rpc_api->rmdir_task.get_dir_name());
                    rmdir(js->rpc_api->req,
                          js->rpc_api->rmdir_task.get_parent_ino(),
                          js->rpc_api->rmdir_task.get_dir_name());
                    break;
                case FUSE_UNLINK:
                    AZLogWarn("[JUKEBOX REISSUE] UNLINK(req={}, parent_ino={}, "
                              "name={}, for_silly_rename={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->unlink_task.get_parent_ino(),
                              js->rpc_api->unlink_task.get_file_name(),
                              js->rpc_api->unlink_task.get_for_silly_rename());
                    unlink(js->rpc_api->req,
                           js->rpc_api->unlink_task.get_parent_ino(),
                           js->rpc_api->unlink_task.get_file_name(),
                           js->rpc_api->unlink_task.get_for_silly_rename());
                    break;
                case FUSE_SYMLINK:
                    AZLogWarn("[JUKEBOX REISSUE] SYMLINK(req={}, link={}, "
                              "parent_ino={}, name={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->symlink_task.get_link(),
                              js->rpc_api->symlink_task.get_parent_ino(),
                              js->rpc_api->symlink_task.get_name());
                    symlink(js->rpc_api->req,
                            js->rpc_api->symlink_task.get_link(),
                            js->rpc_api->symlink_task.get_parent_ino(),
                            js->rpc_api->symlink_task.get_name());
                    break;
                case FUSE_READLINK:
                    AZLogWarn("[JUKEBOX REISSUE] READLINK(req={}, ino={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->readlink_task.get_ino());
                    readlink(js->rpc_api->req,
                             js->rpc_api->readlink_task.get_ino());
                    break;
                case FUSE_RENAME:
                    AZLogWarn("[JUKEBOX REISSUE] RENAME(req={}, parent_ino={}, "
                              "name={}, newparent_ino={}, newname={}, "
                              "silly_rename={}, silly_rename_ino={}, "
                              "oldparent_ino={}, oldname={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->rename_task.get_parent_ino(),
                              js->rpc_api->rename_task.get_name(),
                              js->rpc_api->rename_task.get_newparent_ino(),
                              js->rpc_api->rename_task.get_newname(),
                              js->rpc_api->rename_task.get_silly_rename(),
                              js->rpc_api->rename_task.get_silly_rename_ino(),
                              js->rpc_api->rename_task.get_oldparent_ino(),
                              js->rpc_api->rename_task.get_oldname());
                    rename(js->rpc_api->req,
                           js->rpc_api->rename_task.get_parent_ino(),
                           js->rpc_api->rename_task.get_name(),
                           js->rpc_api->rename_task.get_newparent_ino(),
                           js->rpc_api->rename_task.get_newname(),
                           js->rpc_api->rename_task.get_silly_rename(),
                           js->rpc_api->rename_task.get_silly_rename_ino(),
                           js->rpc_api->rename_task.get_oldparent_ino(),
                           js->rpc_api->rename_task.get_oldname());
                    break;
                case FUSE_READ:
                    AZLogWarn("[JUKEBOX REISSUE] READ(req={}, ino={}, "
                              "size={}, offset={} fi={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->read_task.get_ino(),
                              js->rpc_api->read_task.get_size(),
                              js->rpc_api->read_task.get_offset(),
                              fmt::ptr(js->rpc_api->read_task.get_fuse_file()));
                    jukebox_read(js->rpc_api);
                    break;
                case FUSE_READDIR:
                    AZLogWarn("[JUKEBOX REISSUE] READDIR(req={}, ino={}, "
                              "size={}, off={}, fi={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->readdir_task.get_ino(),
                              js->rpc_api->readdir_task.get_size(),
                              js->rpc_api->readdir_task.get_offset(),
                              fmt::ptr(js->rpc_api->readdir_task.get_fuse_file()));
                    readdir(js->rpc_api->req,
                            js->rpc_api->readdir_task.get_ino(),
                            js->rpc_api->readdir_task.get_size(),
                            js->rpc_api->readdir_task.get_offset(),
                            js->rpc_api->readdir_task.get_fuse_file());
                    break;
                case FUSE_READDIRPLUS:
                    AZLogWarn("[JUKEBOX REISSUE] READDIRPLUS(req={}, ino={}, "
                              "size={}, off={}, fi={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->readdir_task.get_ino(),
                              js->rpc_api->readdir_task.get_size(),
                              js->rpc_api->readdir_task.get_offset(),
                              fmt::ptr(js->rpc_api->readdir_task.get_fuse_file()));
                    readdirplus(js->rpc_api->req,
                                js->rpc_api->readdir_task.get_ino(),
                                js->rpc_api->readdir_task.get_size(),
                                js->rpc_api->readdir_task.get_offset(),
                                js->rpc_api->readdir_task.get_fuse_file());
                    break;
                case FUSE_WRITE:
                    AZLogWarn("[JUKEBOX REISSUE] WRITE(req={}, ino={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->write_task.get_ino());
                    jukebox_write(js->rpc_api);
                    break;
                case FUSE_FLUSH:
                    AZLogWarn("[JUKEBOX REISSUE] COMMIT(req={}, ino={})",
                              fmt::ptr(js->rpc_api->req),
                              js->rpc_api->flush_task.get_ino());
                    jukebox_flush(js->rpc_api);
                    break;
                /* TODO: Add other request types */
                default:
                    AZLogError("Unknown jukebox seed type: {}", (int) js->rpc_api->optype);
                    assert(0);
                    break;
            }

            delete js;
        }
    } while (!shutting_down);
}

void blob_client::put_blob_inode(fuse_ino_t ino, std::shared_ptr<blob_inode> inode)
{
    std::lock_guard<std::shared_mutex> lock(inode_map_lock);
    inode_map[ino] = inode;
    AZLogInfo("Inode with ino %lu added to map", ino);
}

std::shared_ptr<blob_inode> blob_client::create_blob_inode(const std::string& name, const Azure::Storage::Blobs::Models::BlobProperties* props) {
    static std::atomic<fuse_ino_t> next_ino{2};

    auto inode = std::make_shared<blob_inode>(S_IFREG,this,2, name);
    inode->ino = next_ino++;
    inode->blob_name = name;
    inode->size = props->BlobSize;
    inode->last_modified = props->LastModified.ToString();

    std::lock_guard lock(inode_map_lock);
    inode_map[inode->ino] = inode;

    return inode;
}


bool blob_client::init(const std::string& conn_str, const std::string& container)
{
    connection_string = conn_str;
    container_name = container;

    if (!transport.start()) {
        AZLogError("Failed to start the RPC transport.");
        return false;
    }

    rpc_task_helper = rpc_task_helper::get_instance(this);
    containerClient = Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(
                        connection_string, container_name);


    jukebox_thread = std::thread(&blob_client::jukebox_runner, this);
    return true;
}

std::shared_ptr<blob_inode> blob_client::get_blob_inode_from_ino(fuse_ino_t ino) {
    std::shared_lock<std::shared_mutex> lock(inode_map_lock);
    auto it = inode_map.find(ino);
    return (it != inode_map.end()) ? it->second : nullptr;
}

void blob_client::lookup(fuse_req_t req, fuse_ino_t parent_ino, const char* name)
{
    struct rpc_task *tsk = rpc_task_helper->alloc_rpc_task(FUSE_LOOKUP);

    tsk->init_lookup(req, name, parent_ino);
    tsk->run_lookup();

    /*
     * Note: Don't access tsk after this as it may get freed anytime after
     *       the run_lookup() call. This applies to all APIs.
     */
}

void blob_client::getattr(
    fuse_req_t req,
    fuse_ino_t ino,
    struct fuse_file_info *file)
{
    AZLogInfo("Entered in blob client getattr");
    std::shared_ptr<blob_inode> inode_ptr = get_blob_inode_from_ino(ino);

    struct rpc_task *tsk = rpc_task_helper->alloc_rpc_task(FUSE_GETATTR);
    if (!tsk) {
        fuse_reply_err(req, ENOMEM);
        return;
    }

    tsk->init_getattr(req, ino);
    AZLogInfo("Done init getattr");
    tsk->run_getattr();
}

void blob_client::read(
    fuse_req_t req,
    fuse_ino_t ino,
    size_t size,
    off_t off,
    struct fuse_file_info *fi)
{
    struct rpc_task *tsk = rpc_task_helper->alloc_rpc_task(FUSE_READ);
    //struct blob_inode *inode = get_blob_inode_from_ino(ino).get();

    /*
     * Fuse doesn't let us decide the max file size supported, so kernel can
     * technically send us a request for an offset larger than we support.
     * Adjust size to not read beyond the max file size supported.
     * Note that we can pass it down to the Blob NFS server and it'll correctly
     * handle it, but we have many useful asserts, avoid those.
     */
    if ((off + size) > AZNFSC_MAX_FILE_SIZE) {
        const size_t adj_size =
            std::max((off_t) AZNFSC_MAX_FILE_SIZE - off, (off_t) 0);
        if (adj_size == 0) {
            AZLogWarn("[{}] Read beyond maximum file size suported ({}), "
                      "offset={}, size={}, adj_size={}",
                      ino, AZNFSC_MAX_FILE_SIZE, off, size, adj_size);
        }

        size = adj_size;
    }

    tsk->init_read_fe(req, ino, size, off, fi);

    if (size == 0) {
        INC_GBL_STATS(zero_reads, 1);
        tsk->reply_iov(nullptr, 0);
        return;
    }

    tsk->run_read();
}

 void blob_client::flush(fuse_req_t req, fuse_ino_t ino)
  {
      struct rpc_task *tsk = rpc_task_helper->alloc_rpc_task(FUSE_FLUSH);

      tsk->init_flush(req, ino);
      tsk->run_flush();
  }

