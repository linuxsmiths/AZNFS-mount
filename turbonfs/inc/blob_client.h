#ifndef __BLOB_CLIENT_H__
#define __BLOB_CLIENT_H__

#include <azure/storage/blobs.hpp>
#include <optional>
#include <queue>
#include <memory>
#include <shared_mutex>
#include <string>
#include <map>
#include "blob_inode.h"
#include "rpc_transport.h"

struct blob_client {
private:
    struct rpc_transport transport;
    std::string connection_string;
    std::string container_name;
    std::optional<Azure::Storage::Blobs::BlobContainerClient> containerClient;
    std::map<fuse_ino_t, std::shared_ptr<blob_inode>> inode_map;
    mutable std::shared_mutex inode_map_lock;


    std::thread jukebox_thread;
    void jukebox_runner();
    std::queue<struct jukebox_seedinfo*> jukebox_seeds;
    mutable std::mutex jukebox_seeds_lock_39;

        blob_client() :
        transport(this)
    {
    }

    ~blob_client()
    {
        AZLogInfo("~blob_client() called");

        /*
         * shutdown() should have cleared the root_fh.
         */
    }
public:

    bool init(const std::string& conn_str, const std::string& container);
    static blob_client& get_instance()
    {
        static blob_client client;
        return client;
    }

    class rpc_task_helper *rpc_task_helper = nullptr;

    Azure::Storage::Blobs::BlobContainerClient get_container_client() {
        return *containerClient;
    }

    class rpc_task_helper *get_rpc_task_helper()
    {
        return rpc_task_helper;
    }

    std::shared_ptr<blob_inode> get_blob_inode_from_ino(fuse_ino_t ino);
    void getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* file);
        /**
     * Issue a sync GETATTR RPC call to filehandle 'fh' and save the received
     * attributes in 'fattr'.
     * This is to be used internally and not for serving fuse requests.
     */
    bool getattr_sync(const struct nfs_fh3& fh,
                      fuse_ino_t ino,
                      struct fattr3& attr)
                      {
                        AZLogInfo("Entered here");
                      return true;
                      }

    void statfs(fuse_req_t req, fuse_ino_t ino){
                        AZLogInfo("Entered here");
                      }

        void put_blob_inode(fuse_ino_t ino, std::shared_ptr<blob_inode> inode);


    void create(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char *name,
        mode_t mode,
        struct fuse_file_info* file){
                        AZLogInfo("Entered here");
                      }

    void mknod(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char *name,
        mode_t mode){
                        AZLogInfo("Entered here");
                      }

    void mkdir(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char *name,
        mode_t mode){
                        AZLogInfo("Entered here");
                      }

    /**
     * Try to perform silly rename of the given file (parent_ino/name) and
     * return true if silly rename was required (and done), else return false.
     * Note that silly rename is required for the following two cases:
     *
     * 1. When unlinking a file we need to silly rename the file if it has a
     *    non-zero open count.
     *    In this case caller just needs to pass parent_ino and name.
     *    In this case (silly) renaming the to-be-unlinked file is sufficient
     *    in order to serve the unlink requested by the user.
     * 2. When renaming oldparent_ino/old_name to parent_ino/name, after the
     *    rename parent_ino/name will start referring to the file originally
     *    referred by oldparent_ino/old_name and in case parent_ino/name existed
     *    at the time of rename that file would no longer be accessible after
     *    rename, so it's effectively deleted by the server. Hence we need to
     *    silly rename it if it has a non-zero open count.
     *    In this case caller needs to pass parent_ino and name and additionally
     *    oldparent_ino and old_name. The oldparent_ino and old_name are as such
     *    not used by silly rename but since the actual rename is performed when
     *    the silly rename succeeds (from rename_callback()), we need to store
     *    the oldparent_ino and old_name details in the silly rename task.
     *    In this case silly_rename() will do the following:
     *    - silly rename the outgoing file, and if/when silly rename succeeds,
     *      perform actual rename (oldparent_ino/old_name -> parent_ino/name).
     */
    bool silly_rename(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char *name,
        fuse_ino_t oldparent_ino = 0,
        const char *old_name = nullptr){
                        AZLogInfo("Entered here");
                        return true;
                      }

    /**
     * for_silly_rename tells if this unlink() call is being made to delete
     * a silly-renamed file (.nfs_*), as a result of a release() call from
     * fuse that drops the final opencnt on the file. Note that an earlier
     * unlink  of the file would have caused the file to be (silly)renamed to
     * the .nfs_* name and now when the last opencnt is dropped we need to
     * delete the .nfs_* file. Since we hold the parent directory inode refcnt
     * in rename_callback() for silly renamed files, we need to drop the refcnt
     * now.
     */
    void unlink(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char *name,
        bool for_silly_rename){
                        AZLogInfo("Entered here");
                      }

    void rmdir(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char* name){
                        AZLogInfo("Entered here");
                      }

    void symlink(
        fuse_req_t req,
        const char *link,
        fuse_ino_t parent_ino,
        const char *name){
                        AZLogInfo("Entered here");
                      }

    /**
     * silly_rename must be passed as true if this is a silly rename and not
     * rename triggered by user. See silly_rename() for explanation of why and
     * when we need to silly rename a file. If this rename operation is
     * being performed to realize a silly rename, then silly_rename_ino must
     * contain the ino of the file that's being silly renamed.
     * Also in that case oldparent_ino and old_name refer to the source of the
     * actual rename triggered by user.
     *
     * See comments above init_rename() in rpc_task.h.
     */
    void rename(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char *name,
        fuse_ino_t newparent_ino,
        const char *new_name,
        bool silly_rename = false,
        fuse_ino_t silly_rename_ino = 0,
        fuse_ino_t oldparent_ino = 0,
        const char *old_name = nullptr){
                        AZLogInfo("Entered here");
                      }

    void readlink(
        fuse_req_t req,
        fuse_ino_t ino){
                        AZLogInfo("Entered here");
                      }

    void setattr(
        fuse_req_t req,
        fuse_ino_t ino,
        const struct stat* attr,
        int to_set,
        struct fuse_file_info* file){
                        AZLogInfo("Entered here");
                      }

    void lookup(
        fuse_req_t req,
        fuse_ino_t parent_ino,
        const char* name);

    std::shared_ptr<blob_inode> create_blob_inode(const std::string& name, const Azure::Storage::Blobs::Models::BlobProperties* props);


    /**
     * Sync version of lookup().
     * This is to be used internally and not for serving fuse requests.
     * It returns 0 if we are able to get a success response for the
     * LOOKUP RPC that we sent, in that case child_ino will contain the
     * child's fuse inode number.
     * In case of a failed lookup it'll return a +ve errno value.
     */
    int lookup_sync(
        fuse_ino_t parent_ino,
        const char *name,
        fuse_ino_t& child_ino){
                        AZLogInfo("Entered here");
                        return 0;
                      }

    void access(
        fuse_req_t req,
        fuse_ino_t ino,
        int mask){
                        AZLogInfo("Entered here");
                      }

    void write(
        fuse_req_t req,
        fuse_ino_t ino,
        struct fuse_bufvec *bufv,
        size_t size,
        off_t off){
                        AZLogInfo("Entered here");
                      }

    void flush(
        fuse_req_t req,
        fuse_ino_t ino);

    void readdir(
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t off,
        struct fuse_file_info* file){
                        AZLogInfo("Entered here");
                      }

    void readdirplus(
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t off,
        struct fuse_file_info* file){
                        AZLogInfo("Entered here");
                      }

    void read(
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t off,
        struct fuse_file_info *fi);

    void jukebox_read(struct api_task_info *rpc_api);

    void jukebox_write(struct api_task_info *rpc_api);

    void jukebox_flush(struct api_task_info *rpc_api);
    std::atomic<bool> shutting_down = false;


};

static inline
struct blob_client *get_blob_client_from_fuse_req(
    [[maybe_unused]] const fuse_req_t req = nullptr)
{
    return &blob_client::get_instance();
}

#endif /* __BLOB_CLIENT_H__ */
