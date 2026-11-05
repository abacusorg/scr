#ifndef AXL_DFS_H
#define AXL_DFS_H

/* AXL_XFER_DFS: a file under a container registered with AXL_DFS_Register is made by dfs_open
 * and written by dfs_write; any other destination is copied by axl_file_copy. */

/* One transfer's directory handles and counts, shared by its threads */
struct axl_dfs_xfer;
struct axl_dfs_xfer* axl_dfs_xfer_new(void);
void axl_dfs_xfer_free(struct axl_dfs_xfer* xfer);

/* One worker thread's event queue and buffers */
struct axl_dfs_thread;
struct axl_dfs_thread* axl_dfs_thread_new(struct axl_dfs_xfer* xfer);
void axl_dfs_thread_free(struct axl_dfs_thread* thread);

/* axl_file_copy, through libdfs where the destination allows it; *size_checked says the
 * copy confirmed the destination holds the expected `size` bytes */
int axl_dfs_file_copy(
    struct axl_dfs_thread* thread,
    const char* src_file,
    const char* dst_file,
    unsigned long buf_size,
    int resume,
    const unsigned long* size,
    int* size_checked
);

/* Drop this library's reference to the DAOS client */
void axl_dfs_finalize(void);

#endif /* AXL_DFS_H */
