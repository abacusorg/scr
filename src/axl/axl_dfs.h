#ifndef AXL_DFS_H
#define AXL_DFS_H

/* AXL_XFER_DFS: the pthread engine with a copy loop that writes through libdfs.
 *
 * The destination is opened through the kernel as usual, which follows any symlink and
 * lets dfuse create the file.  When the file is on a dfuse mount, dfuse lends its pool,
 * container and file handles through ioctls (the protocol libioil uses), and the data
 * goes out with dfs_write rather than through FUSE.  Any other destination, including
 * one pil4dfs already intercepts, is copied by axl_file_copy. */

/* The DAOS handles one transfer borrows, shared by its threads */
struct axl_dfs_xfer;
struct axl_dfs_xfer* axl_dfs_xfer_new(void);
void axl_dfs_xfer_free(struct axl_dfs_xfer* xfer);

/* One worker thread's event queue and buffers */
struct axl_dfs_thread;
struct axl_dfs_thread* axl_dfs_thread_new(struct axl_dfs_xfer* xfer);
void axl_dfs_thread_free(struct axl_dfs_thread* thread);

/* axl_file_copy, through libdfs where the destination allows it */
int axl_dfs_file_copy(
    struct axl_dfs_thread* thread,
    const char* src_file,
    const char* dst_file,
    unsigned long buf_size,
    int resume
);

/* Drop this library's reference to the DAOS client */
void axl_dfs_finalize(void);

#endif /* AXL_DFS_H */
