#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <daos.h>
#include <daos_fs.h>

#include "axl_internal.h"
#include "axl_dfs.h"

/* The dfuse ioctl protocol, from DAOS src/include/dfuse_ioctl.h (version 7, DAOS 2.6),
 * which daos-devel does not install.  dfuse puts its version in every reply, and a
 * mismatch sends the file to POSIX. */
#define AXL_DFUSE_IOCTL_TYPE        0xA3
#define AXL_DFUSE_IOCTL_VERSION     7
#define AXL_DFUSE_IOCTL_REPLY_CORE  0xC1
#define AXL_DFUSE_IOCTL_REPLY_POH   0xC3
#define AXL_DFUSE_IOCTL_REPLY_COH   0xC4
#define AXL_DFUSE_IOCTL_REPLY_DOH   0xC5
#define AXL_DFUSE_IOCTL_REPLY_DOOH  0xC6
#define AXL_DFUSE_IOCTL_REPLY_SIZE  0xC7
#define AXL_DFUSE_IOCTL_REPLY_DSIZE 0xC8
#define AXL_DFUSE_IOCTL_REPLY_PFILE 0xC9

struct axl_dfuse_il_reply {
    int           fir_version;
    daos_obj_id_t fir_oid;
    uuid_t        fir_pool;
    uuid_t        fir_cont;
    uint64_t      fir_flags;
};

/* sizes of the pool, container and dfs global handles */
struct axl_dfuse_hs_reply {
    int    fsr_version;
    size_t fsr_pool_size;
    size_t fsr_cont_size;
    size_t fsr_dfs_size;
};

/* size of the file object's global handle */
struct axl_dfuse_hsd_reply {
    int    fsr_version;
    size_t fsr_dobj_size;
};

#define AXL_DFUSE_IOCTL_IL \
    _IOR(AXL_DFUSE_IOCTL_TYPE, AXL_DFUSE_IOCTL_REPLY_CORE, struct axl_dfuse_il_reply)
#define AXL_DFUSE_IOCTL_IL_SIZE \
    _IOR(AXL_DFUSE_IOCTL_TYPE, AXL_DFUSE_IOCTL_REPLY_SIZE, struct axl_dfuse_hs_reply)
#define AXL_DFUSE_IOCTL_IL_DSIZE \
    _IOR(AXL_DFUSE_IOCTL_TYPE, AXL_DFUSE_IOCTL_REPLY_DSIZE, struct axl_dfuse_hsd_reply)

/* An ioctl's argument size has 14 bits; dfuse sends a bigger pool handle through a file */
#define AXL_DFUSE_IOCTL_MAX (16 * 1024)

/* why a file went through POSIX when no errno says it */
#define AXL_DFS_NO_EQ (-1)

/* DAOS's own calls return -DER_* codes, which have no public errno mapping */
static int axl_dfs_der(const char* what, int der)
{
    AXL_ERR("%s failed: DER %d", what, der);
    return EIO;
}

/* ---------------------------------------------------------------------------------------
 * The DAOS client.  daos_init and daos_fini are reference counted, so this reference
 * coexists with pil4dfs's. */

static pthread_mutex_t axl_dfs_init_lock = PTHREAD_MUTEX_INITIALIZER;
static int axl_dfs_inited = 0;
static int axl_dfs_init_failed = 0;

static int axl_dfs_init(void)
{
    pthread_mutex_lock(&axl_dfs_init_lock);
    if (! axl_dfs_inited && ! axl_dfs_init_failed) {
        int rc = daos_init();
        if (rc == 0) {
            axl_dfs_inited = 1;
        } else {
            /* not retried, or every file would pay for it again */
            axl_dfs_init_failed = 1;
            AXL_ERR("daos_init failed (DER %d); DFS transfers will copy through POSIX", rc);
        }
    }
    int inited = axl_dfs_inited;
    pthread_mutex_unlock(&axl_dfs_init_lock);
    return inited;
}

void axl_dfs_finalize(void)
{
    pthread_mutex_lock(&axl_dfs_init_lock);
    if (axl_dfs_inited) {
        int rc = daos_fini();
        if (rc != 0) {
            AXL_ERR("daos_fini failed: DER %d", rc);
        }
        axl_dfs_inited = 0;
    }
    axl_dfs_init_failed = 0;
    pthread_mutex_unlock(&axl_dfs_init_lock);
}

/* ---------------------------------------------------------------------------------------
 * Handles borrowed from dfuse.  Every checkpoint container has its own dfuse, so each
 * container brings its own pool handle too. */

struct axl_dfs_cont {
    struct axl_dfs_cont* next;
    uuid_t uuid;
    daos_handle_t poh;
    daos_handle_t coh;
    dfs_t* dfs;
};

struct axl_dfs_xfer {
    pthread_mutex_t lock;
    struct axl_dfs_cont* conts;

    unsigned long files_dfs;
    unsigned long long bytes_dfs;
    unsigned long files_posix;
    int posix_reason; /* errno that sent the first file to POSIX, or AXL_DFS_NO_EQ */
};

/* Imported handles are local copies, so releasing them sends no RPC and leaves dfuse's
 * own handles open. */
static void axl_dfs_cont_release(struct axl_dfs_cont* c)
{
    int rc;
    if (c->dfs != NULL && (rc = dfs_umount(c->dfs)) != 0) {
        AXL_ERR("dfs_umount failed: %s", strerror(rc));
    }
    if (daos_handle_is_valid(c->coh) && (rc = daos_cont_close(c->coh, NULL)) != 0) {
        AXL_ERR("daos_cont_close failed: DER %d", rc);
    }
    if (daos_handle_is_valid(c->poh) && (rc = daos_pool_disconnect(c->poh, NULL)) != 0) {
        AXL_ERR("daos_pool_disconnect failed: DER %d", rc);
    }
    c->dfs = NULL;
    c->coh = DAOS_HDL_INVAL;
    c->poh = DAOS_HDL_INVAL;
}

/* Fetch a global handle of `size` bytes with reply number `nr`; the caller frees it */
static int axl_dfs_fetch(int fd, int nr, size_t size, d_iov_t* iov)
{
    if (size == 0 || size >= AXL_DFUSE_IOCTL_MAX) {
        return EOVERFLOW;
    }
    void* buf = malloc(size);
    if (buf == NULL) {
        return ENOMEM;
    }
    if (ioctl(fd, _IOC(_IOC_READ, AXL_DFUSE_IOCTL_TYPE, nr, size), buf) != 0) {
        int err = errno;
        free(buf);
        return err;
    }
    d_iov_set(iov, buf, size);
    return 0;
}

/* Read all of a small file; the caller frees it */
static int axl_dfs_slurp(const char* path, d_iov_t* iov)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return errno;
    }
    int rc = 0;
    void* buf = NULL;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        rc = errno;
    } else if (st.st_size <= 0) {
        rc = EIO;
    } else if ((buf = malloc(st.st_size)) == NULL) {
        rc = ENOMEM;
    } else if (axl_read_attempt(path, fd, buf, st.st_size) != st.st_size) {
        rc = EIO;
    }
    close(fd);
    if (rc != 0) {
        free(buf);
        return rc;
    }
    d_iov_set(iov, buf, st.st_size);
    return 0;
}

/* The pool handle carries the pool map and is often too big for an ioctl, in which case
 * dfuse writes it to a file in /tmp and replies with the file's name. */
static int axl_dfs_fetch_pool(int fd, size_t size, daos_handle_t* poh)
{
    d_iov_t iov;
    int rc;
    if (size < AXL_DFUSE_IOCTL_MAX) {
        rc = axl_dfs_fetch(fd, AXL_DFUSE_IOCTL_REPLY_POH, size, &iov);
    } else {
        char name[128] = "";
        unsigned long cmd =
            _IOC(_IOC_READ, AXL_DFUSE_IOCTL_TYPE, AXL_DFUSE_IOCTL_REPLY_PFILE, sizeof(name));
        if (ioctl(fd, cmd, name) != 0) {
            return errno;
        }
        name[sizeof(name) - 1] = '\0';
        rc = axl_dfs_slurp(name, &iov);
        unlink(name);
    }
    if (rc != 0) {
        return rc;
    }
    int der = daos_pool_global2local(iov, poh);
    free(iov.iov_buf);
    return der ? axl_dfs_der("daos_pool_global2local", der) : 0;
}

/* Import the pool, container and dfs handles of the dfuse that serves `fd` */
static int axl_dfs_import(int fd, struct axl_dfs_cont* c)
{
    int rc = EAGAIN;
    int tries;
    /* dfuse answers EAGAIN when a handle has grown since the size query */
    for (tries = 0; tries < 3 && rc == EAGAIN; tries++) {
        axl_dfs_cont_release(c);

        struct axl_dfuse_hs_reply hs;
        if (ioctl(fd, AXL_DFUSE_IOCTL_IL_SIZE, &hs) != 0) {
            rc = errno;
            break;
        }
        if (hs.fsr_version != AXL_DFUSE_IOCTL_VERSION) {
            rc = EPROTO;
            break;
        }

        rc = axl_dfs_fetch_pool(fd, hs.fsr_pool_size, &c->poh);
        if (rc != 0) {
            continue;
        }

        d_iov_t iov;
        rc = axl_dfs_fetch(fd, AXL_DFUSE_IOCTL_REPLY_COH, hs.fsr_cont_size, &iov);
        if (rc != 0) {
            continue;
        }
        int der = daos_cont_global2local(c->poh, iov, &c->coh);
        free(iov.iov_buf);
        if (der != 0) {
            rc = axl_dfs_der("daos_cont_global2local", der);
            continue;
        }

        rc = axl_dfs_fetch(fd, AXL_DFUSE_IOCTL_REPLY_DOH, hs.fsr_dfs_size, &iov);
        if (rc != 0) {
            continue;
        }
        rc = dfs_global2local(c->poh, c->coh, 0, iov, &c->dfs);
        free(iov.iov_buf);
    }
    if (rc != 0) {
        axl_dfs_cont_release(c);
    }
    return rc;
}

/* The dfs mount of the container `fd` is in.  The transfer's lock is held through an
 * import, so a container is imported once rather than once per thread. */
static int axl_dfs_cont_get(
    struct axl_dfs_xfer* x,
    int fd,
    const struct axl_dfuse_il_reply* il,
    dfs_t** dfs)
{
    int rc = 0;
    pthread_mutex_lock(&x->lock);
    struct axl_dfs_cont* c = x->conts;
    while (c != NULL && memcmp(c->uuid, il->fir_cont, sizeof(uuid_t)) != 0) {
        c = c->next;
    }
    if (c == NULL) {
        c = calloc(1, sizeof(*c));
        if (c == NULL) {
            rc = ENOMEM;
        } else {
            memcpy(c->uuid, il->fir_cont, sizeof(uuid_t));
            rc = axl_dfs_import(fd, c);
            if (rc == 0) {
                c->next = x->conts;
                x->conts = c;
            } else {
                free(c);
                c = NULL;
            }
        }
    }
    if (c != NULL) {
        *dfs = c->dfs;
    }
    pthread_mutex_unlock(&x->lock);
    return rc;
}

/* The libdfs view of the dfuse file open at `fd`, or the errno for why there is none.
 * Off dfuse the first ioctl fails with ENOTTY, and pil4dfs answers ENOTSUP for a file
 * it intercepts. */
static int axl_dfs_open_obj(struct axl_dfs_xfer* x, int fd, dfs_t** dfs, dfs_obj_t** obj)
{
    struct axl_dfuse_il_reply il;
    if (ioctl(fd, AXL_DFUSE_IOCTL_IL, &il) != 0) {
        return errno;
    }
    if (il.fir_version != AXL_DFUSE_IOCTL_VERSION) {
        return EPROTO;
    }

    int rc = axl_dfs_cont_get(x, fd, &il, dfs);
    if (rc != 0) {
        return rc;
    }

    struct axl_dfuse_hsd_reply hsd;
    if (ioctl(fd, AXL_DFUSE_IOCTL_IL_DSIZE, &hsd) != 0) {
        return errno;
    }
    if (hsd.fsr_version != AXL_DFUSE_IOCTL_VERSION) {
        return EPROTO;
    }

    d_iov_t iov;
    rc = axl_dfs_fetch(fd, AXL_DFUSE_IOCTL_REPLY_DOOH, hsd.fsr_dobj_size, &iov);
    if (rc != 0) {
        return rc;
    }
    rc = dfs_obj_global2local(*dfs, 0, iov, obj);
    free(iov.iov_buf);
    return rc;
}

struct axl_dfs_xfer* axl_dfs_xfer_new(void)
{
    struct axl_dfs_xfer* x = calloc(1, sizeof(*x));
    if (x == NULL) {
        return NULL;
    }
    if (pthread_mutex_init(&x->lock, NULL) != 0) {
        free(x);
        return NULL;
    }
    return x;
}

void axl_dfs_xfer_free(struct axl_dfs_xfer* x)
{
    if (x == NULL) {
        return;
    }
    while (x->conts != NULL) {
        struct axl_dfs_cont* c = x->conts;
        x->conts = c->next;
        axl_dfs_cont_release(c);
        free(c);
    }

    AXL_DBG(1, "DFS transfer: %lu files (%llu bytes) through libdfs, %lu through POSIX",
        x->files_dfs, x->bytes_dfs, x->files_posix);

    /* Data that should have bypassed FUSE went through it, or through pil4dfs; one rank
     * says so, since every rank shares the configuration that decides it. */
    if (x->files_posix > 0 && axl_rank <= 0) {
        AXL_ERR("DFS transfer copied %lu of %lu files through POSIX, not libdfs; first: %s",
            x->files_posix, x->files_posix + x->files_dfs,
            x->posix_reason == AXL_DFS_NO_EQ ? "no usable DAOS event queue"
                                             : strerror(x->posix_reason));
    }

    pthread_mutex_destroy(&x->lock);
    free(x);
}

static void axl_dfs_count(struct axl_dfs_xfer* x, int posix_reason, unsigned long long bytes)
{
    if (x == NULL) {
        return;
    }
    pthread_mutex_lock(&x->lock);
    if (posix_reason != 0) {
        if (x->files_posix++ == 0) {
            x->posix_reason = posix_reason;
        }
    } else {
        x->files_dfs++;
        x->bytes_dfs += bytes;
    }
    pthread_mutex_unlock(&x->lock);
}

/* ---------------------------------------------------------------------------------------
 * One worker thread.  Its own event queue has its own network context, so the threads
 * never contend to make progress.  Two writes are in flight at a time, each buffer
 * refilled while the other's write runs; the same two buffers serve the whole transfer,
 * so the NIC's registration cache holds them rather than a new one per file. */

struct axl_dfs_thread {
    struct axl_dfs_xfer* xfer;
    int usable;          /* 0 sends every file to POSIX */
    int stuck;           /* an event never completed: its buffer and queue may be in use */
    daos_handle_t eqh;
    daos_event_t ev[2];
    int ev_inited[2];
    char* buf[2];
    unsigned long buf_size;
};

struct axl_dfs_thread* axl_dfs_thread_new(struct axl_dfs_xfer* xfer)
{
    struct axl_dfs_thread* t = calloc(1, sizeof(*t));
    if (t == NULL) {
        return NULL;
    }
    t->xfer = xfer;
    t->eqh = DAOS_HDL_INVAL;
    if (! axl_dfs_init()) {
        return t;
    }

    int rc = daos_eq_create(&t->eqh);
    if (rc != 0) {
        AXL_ERR("daos_eq_create failed: DER %d", rc);
        t->eqh = DAOS_HDL_INVAL;
        return t;
    }
    int i;
    for (i = 0; i < 2; i++) {
        rc = daos_event_init(&t->ev[i], t->eqh, NULL);
        if (rc != 0) {
            AXL_ERR("daos_event_init failed: DER %d", rc);
            return t;
        }
        t->ev_inited[i] = 1;
    }
    t->usable = 1;
    return t;
}

void axl_dfs_thread_free(struct axl_dfs_thread* t)
{
    if (t == NULL) {
        return;
    }
    if (! t->stuck) {
        int i;
        for (i = 0; i < 2; i++) {
            if (t->ev_inited[i]) {
                daos_event_fini(&t->ev[i]);
            }
            free(t->buf[i]);
        }
        if (daos_handle_is_valid(t->eqh)) {
            int rc = daos_eq_destroy(t->eqh, 0);
            if (rc != 0) {
                AXL_ERR("daos_eq_destroy failed: DER %d", rc);
            }
        }
    }
    free(t);
}

static int axl_dfs_buffers(struct axl_dfs_thread* t, unsigned long size)
{
    if (t->buf_size == size) {
        return 0;
    }
    int i;
    for (i = 0; i < 2; i++) {
        free(t->buf[i]);
        t->buf[i] = NULL;
    }
    t->buf_size = 0;
    for (i = 0; i < 2; i++) {
        t->buf[i] = malloc(size);
        if (t->buf[i] == NULL) {
            return ENOMEM;
        }
    }
    t->buf_size = size;
    return 0;
}

/* The errno of the write in flight on `slot` once it completes */
static int axl_dfs_wait(struct axl_dfs_thread* t, int slot)
{
    bool done = false;
    while (! done) {
        int rc = daos_event_test(&t->ev[slot], DAOS_EQ_WAIT, &done);
        if (rc != 0) {
            AXL_ERR("daos_event_test failed: DER %d", rc);
            t->stuck = 1;
            t->usable = 0;
            return EIO;
        }
    }
    return t->ev[slot].ev_error;
}

int axl_dfs_file_copy(
    struct axl_dfs_thread* t,
    const char* src_file,
    const char* dst_file,
    unsigned long buf_size,
    int resume)
{
    if (t == NULL || ! t->usable) {
        axl_dfs_count(t ? t->xfer : NULL, AXL_DFS_NO_EQ, 0);
        return axl_file_copy(src_file, dst_file, buf_size, resume);
    }

    int src_fd = axl_open(src_file, O_RDONLY);
    if (src_fd < 0) {
        AXL_ERR("Opening file to copy: axl_open(%s) errno=%d %s",
            src_file, errno, strerror(errno)
        );
        return AXL_FAILURE;
    }

    /* Through the kernel, which follows the prefix's symlinks into the container and has
     * dfuse create the file.  Without O_TRUNC, a resumed copy continues after what is
     * already there. */
    int flags = O_WRONLY | O_CREAT | (resume ? 0 : O_TRUNC);
    int dst_fd = axl_open(dst_file, flags, axl_getmode(1, 1, 0));
    if (dst_fd < 0) {
        AXL_ERR("Opening file for writing: axl_open(%s) errno=%d %s",
            dst_file, errno, strerror(errno)
        );
        close(src_fd);
        return AXL_FAILURE;
    }

    dfs_t* dfs = NULL;
    dfs_obj_t* obj = NULL;
    int why = axl_dfs_buffers(t, buf_size);
    if (why == 0) {
        why = axl_dfs_open_obj(t->xfer, dst_fd, &dfs, &obj);
    }
    if (why != 0) {
        /* nothing written yet, so the ordinary copy starts from a clean slate */
        close(dst_fd);
        close(src_fd);
        AXL_DBG(2, "Copying %s through POSIX: %s", dst_file, strerror(why));
        axl_dfs_count(t->xfer, why, 0);
        return axl_file_copy(src_file, dst_file, buf_size, resume);
    }

    int rc = AXL_SUCCESS;
    daos_off_t off = 0;
    if (resume) {
        daos_size_t size = 0;
        int err = dfs_get_size(dfs, obj, &size);
        if (err != 0 || lseek(src_fd, (off_t) size, SEEK_SET) != (off_t) size) {
            AXL_ERR("Resuming %s: dfs_get_size %s, errno=%d %s",
                dst_file, strerror(err), errno, strerror(errno)
            );
            rc = AXL_FAILURE;
        }
        off = size;
    }

    /* referenced by DAOS until their writes complete */
    d_iov_t iov[2];
    d_sg_list_t sgl[2];
    int inflight[2] = {0, 0};
    unsigned long long bytes = 0;
    int slot = 0;
    while (rc == AXL_SUCCESS) {
        if (inflight[slot]) {
            inflight[slot] = 0;
            int err = axl_dfs_wait(t, slot);
            if (err != 0) {
                AXL_ERR("Writing %s: %s", dst_file, strerror(err));
                rc = AXL_FAILURE;
                break;
            }
        }

        ssize_t nread = axl_read_attempt(src_file, src_fd, t->buf[slot], buf_size);
        if (nread < 0) {
            rc = AXL_FAILURE;
            break;
        }
        if (nread == 0) {
            break;
        }

        d_iov_set(&iov[slot], t->buf[slot], (size_t) nread);
        sgl[slot].sg_nr = 1;
        sgl[slot].sg_nr_out = 0;
        sgl[slot].sg_iovs = &iov[slot];
        /* an error here means the event never launched */
        int err = dfs_write(dfs, obj, &sgl[slot], off, &t->ev[slot]);
        if (err != 0) {
            AXL_ERR("dfs_write of %s at offset %llu: %s",
                dst_file, (unsigned long long) off, strerror(err)
            );
            rc = AXL_FAILURE;
            break;
        }
        inflight[slot] = 1;
        off += nread;
        bytes += nread;

        /* a short read means the end of the file, as in axl_file_copy */
        if ((unsigned long) nread < buf_size) {
            break;
        }
        slot ^= 1;
    }

    /* Both writes must land before their buffers, iovs and file handle are let go */
    int i;
    for (i = 0; i < 2 && ! t->stuck; i++) {
        if (inflight[i]) {
            int err = axl_dfs_wait(t, i);
            if (err != 0) {
                AXL_ERR("Writing %s: %s", dst_file, strerror(err));
                rc = AXL_FAILURE;
            }
        }
    }

    /* a stuck write may still use the handle */
    if (! t->stuck) {
        dfs_release(obj);
    }

    /* A completed dfs_write is durable, so there is nothing for fsync to flush */
    if (close(dst_fd) != 0) {
        AXL_ERR("Closing %s: errno=%d %s", dst_file, errno, strerror(errno));
        rc = AXL_FAILURE;
    }
    close(src_fd);

    if (rc == AXL_SUCCESS) {
        axl_dfs_count(t->xfer, 0, bytes);
    } else {
        /* A failed write can leave a hole under a full-size file, so the size check in
         * AXL_Wait might pass it; remove it. */
        axl_file_unlink(dst_file);
    }
    return rc;
}
