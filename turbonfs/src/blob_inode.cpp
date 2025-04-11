#include <chrono>
#include "blob_inode.h"
#include "blob_client.h"
#include "rpc_task.h"


blob_inode::blob_inode(uint32_t _file_type, struct blob_client *_client, fuse_ino_t inode, const std::string& name)
        : 
        file_type(_file_type),
        client(_client),
        ino(inode), 
        blob_name(name)
         {
            AZLogDebug("NEW inode created {}", inode);
         }


blob_inode::~blob_inode()
{}

void blob_inode::decref(size_t cnt, bool forget_expected)
{
    // Assuming cnt is an atomic and forget_expected is an int64_t
    if ((int64_t)cnt > (int64_t)forget_expected) {
        AZLogInfo("Decrementing ref count, cnt: %ld, forget_expected: %ld", cnt, forget_expected);
    }

    // Create a shared pointer from 'this' (current instance of blob_inode)
    std::shared_ptr<blob_inode> shared_inode = std::shared_ptr<blob_inode>(this);

    // Put the blob inode into the client using the shared pointer
    client->put_blob_inode(this->ino, shared_inode);
}

