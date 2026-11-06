#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <daos.h>
#include <daos_fs.h>

#include "axl_internal.h"
#include "axl_dfs.h"

/* why a file went through POSIX when no errno says it */
#define AXL_DFS_NO_EQ  (-1)
/* no registered container holds its directory: a plain POSIX destination */
#define AXL_DFS_PLAIN  (-2)

/* daos_init and daos_fini are reference counted, so this reference coexists with pil4dfs's */

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

struct axl_dfs_dir {
    struct axl_dfs_dir* next;
    char* path;
    int why;            /* 0, or why its files go through POSIX */
    dfs_t* dfs;
    dfs_obj_t* obj;
};

struct axl_dfs_xfer {
    pthread_mutex_t lock;
    struct axl_dfs_dir* dirs;
    /* read once on the main thread: axl_getmode reads the umask by setting it */
    mode_t mode;

    unsigned long files_dfs;
    unsigned long long bytes_dfs;
    unsigned long files_posix;    /* through POSIX though their container is registered */
    int posix_reason;             /* why the first of those did */
    unsigned long files_plain;    /* under no registered container */
};

/* Containers registered by AXL_DFS_Register */
struct axl_dfs_reg {
    struct axl_dfs_reg* next;
    char* path;
    char* cont_path;
    dfs_t* dfs;
};

static pthread_mutex_t axl_dfs_reg_lock = PTHREAD_MUTEX_INITIALIZER;
static struct axl_dfs_reg* axl_dfs_regs = NULL;

/* `p` without repeated or trailing slashes or "." components; the caller frees it */
static char* axl_dfs_norm(const char* p)
{
    char* out = malloc(strlen(p) + 2);
    if (out == NULL) {
        return NULL;
    }
    size_t o = 0;
    const char* s = p;
    while (*s != '\0') {
        while (*s == '/') {
            s++;
        }
        const char* e = s;
        while (*e != '\0' && *e != '/') {
            e++;
        }
        size_t len = (size_t) (e - s);
        if (len > 0 && !(len == 1 && s[0] == '.')) {
            if (o > 0 || p[0] == '/') {
                out[o++] = '/';
            }
            memcpy(out + o, s, len);
            o += len;
        }
        s = e;
    }
    if (o == 0) {
        out[o++] = (p[0] == '/') ? '/' : '.';
    }
    out[o] = '\0';
    return out;
}

/* The registered container holding `path`, and `path` inside it (the caller frees *rel), or
 * ENOENT if none holds it */
static int axl_dfs_reg_find(const char* path, dfs_t** dfs, char** rel)
{
    char* p = axl_dfs_norm(path);
    if (p == NULL) {
        return ENOMEM;
    }
    int rc = ENOENT;
    pthread_mutex_lock(&axl_dfs_reg_lock);
    struct axl_dfs_reg* best = NULL;
    size_t best_len = 0;
    struct axl_dfs_reg* r;
    for (r = axl_dfs_regs; r != NULL; r = r->next) {
        size_t n = strlen(r->path);
        if (strncmp(p, r->path, n) == 0 && (p[n] == '\0' || p[n] == '/') && n >= best_len) {
            best = r;
            best_len = n;
        }
    }
    if (best != NULL) {
        const char* rest = p + best_len;   /* "" or "/..." */
        const char* base = strcmp(best->cont_path, "/") == 0 && rest[0] != '\0' ? "" : best->cont_path;
        *rel = malloc(strlen(base) + strlen(rest) + 1);
        if (*rel == NULL) {
            rc = ENOMEM;
        } else {
            strcpy(*rel, base);
            strcat(*rel, rest);
            *dfs = best->dfs;
            rc = 0;
        }
    }
    pthread_mutex_unlock(&axl_dfs_reg_lock);
    free(p);
    return rc;
}

/* The registration of `norm` (to `dfs`, unless that is NULL), taken off the list */
static struct axl_dfs_reg* axl_dfs_reg_unlink_locked(const char* norm, dfs_t* dfs)
{
    struct axl_dfs_reg** pr = &axl_dfs_regs;
    while (*pr != NULL &&
           (strcmp((*pr)->path, norm) != 0 || (dfs != NULL && (*pr)->dfs != dfs))) {
        pr = &(*pr)->next;
    }
    struct axl_dfs_reg* r = *pr;
    if (r != NULL) {
        *pr = r->next;
    }
    return r;
}

static void axl_dfs_reg_free(struct axl_dfs_reg* r)
{
    if (r != NULL) {
        free(r->path);
        free(r->cont_path);
        free(r);
    }
}

int AXL_DFS_Register(const char* path, struct dfs* dfs, const char* cont_path)
{
    if (path == NULL || dfs == NULL || cont_path == NULL || cont_path[0] != '/') {
        AXL_ERR("AXL_DFS_Register needs a path, a dfs mount and an absolute container path");
        return AXL_FAILURE;
    }
    struct axl_dfs_reg* r = calloc(1, sizeof(*r));
    char* p = axl_dfs_norm(path);
    char* c = axl_dfs_norm(cont_path);
    if (r == NULL || p == NULL || c == NULL) {
        free(r);
        free(p);
        free(c);
        return AXL_FAILURE;
    }
    r->path = p;
    r->cont_path = c;
    r->dfs = dfs;
    pthread_mutex_lock(&axl_dfs_reg_lock);
    struct axl_dfs_reg* old = axl_dfs_reg_unlink_locked(p, NULL);
    r->next = axl_dfs_regs;
    axl_dfs_regs = r;
    pthread_mutex_unlock(&axl_dfs_reg_lock);
    axl_dfs_reg_free(old);
    return AXL_SUCCESS;
}

int AXL_DFS_Unregister(const char* path, struct dfs* dfs)
{
    char* p = axl_dfs_norm(path);
    if (p == NULL) {
        return AXL_FAILURE;
    }
    pthread_mutex_lock(&axl_dfs_reg_lock);
    struct axl_dfs_reg* r = axl_dfs_reg_unlink_locked(p, dfs);
    pthread_mutex_unlock(&axl_dfs_reg_lock);
    free(p);
    if (r == NULL) {
        return AXL_FAILURE;
    }
    axl_dfs_reg_free(r);
    return AXL_SUCCESS;
}

int AXL_DFS_Stat(const char* path, struct stat* st)
{
    dfs_t* dfs = NULL;
    char* rel = NULL;
    int rc = axl_dfs_reg_find(path, &dfs, &rel);
    if (rc == ENOENT) {
        return AXL_DFS_UNREGISTERED;
    }
    if (rc == 0) {
        dfs_obj_t* obj = NULL;
        rc = dfs_lookup(dfs, rel, O_RDONLY, &obj, NULL, st);
        if (rc == 0) {
            dfs_release(obj);
        }
        free(rel);
    }
    if (rc != 0) {
        errno = rc;
        return AXL_FAILURE;
    }
    return AXL_SUCCESS;
}

int AXL_DFS_Mkdir(const char* dir, mode_t mode)
{
    dfs_t* dfs = NULL;
    char* rel = NULL;
    int rc = axl_dfs_reg_find(dir, &dfs, &rel);
    if (rc == ENOENT) {
        return AXL_DFS_UNREGISTERED;
    }
    if (rc != 0) {
        return AXL_FAILURE;
    }

    /* one component at a time from the container's root */
    dfs_obj_t* parent = NULL;
    char* save = NULL;
    char* name = strtok_r(rel, "/", &save);
    while (name != NULL && rc == 0) {
        char* next = strtok_r(NULL, "/", &save);
        rc = dfs_mkdir(dfs, parent, name, mode, 0);
        if (rc == EEXIST) {
            rc = 0;
        }
        if (rc == 0 && next != NULL) {
            dfs_obj_t* child = NULL;
            rc = dfs_lookup_rel(dfs, parent, name, O_RDWR, &child, NULL, NULL);
            if (parent != NULL) {
                dfs_release(parent);
            }
            parent = child;
        }
        if (rc != 0) {
            AXL_ERR("Making directory %s: %s at %s", dir, strerror(rc), name);
        }
        name = next;
    }
    if (parent != NULL) {
        dfs_release(parent);
    }
    free(rel);
    return rc == 0 ? AXL_SUCCESS : AXL_FAILURE;
}

/* The handle of directory `path` in the registered container that holds it, else why its
 * files go through POSIX.  Either is found once per transfer. */
static int axl_dfs_dir_get(
    struct axl_dfs_xfer* x,
    const char* path,
    dfs_t** dfs,
    dfs_obj_t** obj)
{
    pthread_mutex_lock(&x->lock);
    struct axl_dfs_dir* d = x->dirs;
    while (d != NULL && strcmp(d->path, path) != 0) {
        d = d->next;
    }
    if (d == NULL) {
        d = calloc(1, sizeof(*d));
        char* p = strdup(path);
        if (d == NULL || p == NULL) {
            free(d);
            free(p);
            pthread_mutex_unlock(&x->lock);
            return ENOMEM;
        }
        d->path = p;
        char* rel = NULL;
        int reg = axl_dfs_reg_find(path, &d->dfs, &rel);
        if (reg == 0) {
            /* read-write, since creating a file writes its entry into the directory */
            d->why = dfs_lookup(d->dfs, rel, O_RDWR, &d->obj, NULL, NULL);
            free(rel);
        } else {
            d->why = (reg == ENOENT) ? AXL_DFS_PLAIN : reg;
        }
        d->next = x->dirs;
        x->dirs = d;
    }
    int why = d->why;
    *dfs = d->dfs;
    *obj = d->obj;
    pthread_mutex_unlock(&x->lock);
    return why;
}

static const char* axl_dfs_reason(int why)
{
    switch (why) {
    case AXL_DFS_NO_EQ:
        return "no usable DAOS event queue";
    case AXL_DFS_PLAIN:
        return "no registered container holds it";
    default:
        return strerror(why);
    }
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
    x->mode = axl_getmode(1, 1, 0);
    return x;
}

void axl_dfs_xfer_free(struct axl_dfs_xfer* x)
{
    if (x == NULL) {
        return;
    }
    /* the directories' handles belong to the registered mounts */
    while (x->dirs != NULL) {
        struct axl_dfs_dir* d = x->dirs;
        x->dirs = d->next;
        if (d->obj != NULL) {
            dfs_release(d->obj);
        }
        free(d->path);
        free(d);
    }

    AXL_DBG(1, "DFS transfer: %lu files (%llu bytes) through libdfs, %lu through POSIX, "
        "%lu under no registered container", x->files_dfs, x->bytes_dfs, x->files_posix,
        x->files_plain);

    /* Files a registered container holds went through POSIX instead; one rank says so,
     * since every rank shares the configuration that decides it. */
    if (x->files_posix > 0 && axl_rank <= 0) {
        AXL_ERR("DFS transfer copied %lu of %lu files through POSIX, not libdfs; first: %s",
            x->files_posix, x->files_posix + x->files_dfs, axl_dfs_reason(x->posix_reason));
    }

    pthread_mutex_destroy(&x->lock);
    free(x);
}

static void axl_dfs_count(
    struct axl_dfs_xfer* x,
    int posix_reason,
    unsigned long long bytes)
{
    if (x == NULL) {
        return;
    }
    pthread_mutex_lock(&x->lock);
    if (posix_reason == AXL_DFS_PLAIN) {
        x->files_plain++;
    } else if (posix_reason != 0) {
        if (x->files_posix++ == 0) {
            x->posix_reason = posix_reason;
        }
    } else {
        x->files_dfs++;
        x->bytes_dfs += bytes;
    }
    pthread_mutex_unlock(&x->lock);
}

/* Each thread's event queue has its own network context, so threads never contend to make
 * progress.  Its two buffers alternate, and serving every file keeps them in the MR cache. */

struct axl_dfs_thread {
    struct axl_dfs_xfer* xfer;
    int usable;          /* 0 sends every file to POSIX */
    int stuck;           /* an event never completed: its buffer, sgl and queue may be in use */
    daos_handle_t eqh;
    daos_event_t ev[2];
    int ev_inited[2];
    d_iov_t iov[2];      /* DAOS keeps the sgl by pointer until its write completes */
    d_sg_list_t sgl[2];
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
    /* a stuck write may still reference the events, sgl and buffers, all in here */
    if (t->stuck) {
        return;
    }
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
    int resume,
    const unsigned long* size,
    int* size_checked)
{
    *size_checked = 0;
    if (t == NULL || ! t->usable) {
        axl_dfs_count(t ? t->xfer : NULL, AXL_DFS_NO_EQ, 0);
        return axl_file_copy(src_file, dst_file, buf_size, resume);
    }

    /* SCR has made the destination's directory */
    const char* slash = strrchr(dst_file, '/');
    const char* name = slash != NULL ? slash + 1 : dst_file;
    char* dir = slash == NULL      ? strdup(".")
              : slash == dst_file ? strdup("/")
                                  : strndup(dst_file, (size_t) (slash - dst_file));
    dfs_t* dfs = NULL;
    dfs_obj_t* parent = NULL;
    int why = dir != NULL ? axl_dfs_dir_get(t->xfer, dir, &dfs, &parent) : ENOMEM;
    free(dir);
    if (why == 0) {
        why = axl_dfs_buffers(t, buf_size);
    }
    if (why != 0) {
        AXL_DBG(2, "Copying %s through POSIX: %s", dst_file, axl_dfs_reason(why));
        axl_dfs_count(t->xfer, why, 0);
        return axl_file_copy(src_file, dst_file, buf_size, resume);
    }

    int src_fd = axl_open(src_file, O_RDONLY);
    if (src_fd < 0) {
        AXL_ERR("Opening file to copy: axl_open(%s) errno=%d %s",
            src_file, errno, strerror(errno)
        );
        return AXL_FAILURE;
    }

    /* without O_TRUNC, a resumed copy continues after what is already there */
    dfs_obj_t* obj = NULL;
    int flags = O_RDWR | O_CREAT | (resume ? 0 : O_TRUNC);
    int err = dfs_open(dfs, parent, name, S_IFREG | t->xfer->mode, flags, 0, 0, NULL, &obj);
    if (err != 0) {
        AXL_ERR("Creating %s: dfs_open %s", dst_file, strerror(err));
        close(src_fd);
        return AXL_FAILURE;
    }

    int rc = AXL_SUCCESS;
    daos_off_t off = 0;
    if (resume) {
        daos_size_t done = 0;
        err = dfs_get_size(dfs, obj, &done);
        if (err != 0 || lseek(src_fd, (off_t) done, SEEK_SET) != (off_t) done) {
            AXL_ERR("Resuming %s: dfs_get_size %s, errno=%d %s",
                dst_file, strerror(err), errno, strerror(errno)
            );
            rc = AXL_FAILURE;
        }
        off = done;
    }

    int inflight[2] = {0, 0};
    unsigned long long bytes = 0;
    int slot = 0;
    while (rc == AXL_SUCCESS) {
        if (inflight[slot]) {
            inflight[slot] = 0;
            err = axl_dfs_wait(t, slot);
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

        d_iov_set(&t->iov[slot], t->buf[slot], (size_t) nread);
        t->sgl[slot].sg_nr = 1;
        t->sgl[slot].sg_nr_out = 0;
        t->sgl[slot].sg_iovs = &t->iov[slot];
        /* an error here means the event never launched */
        err = dfs_write(dfs, obj, &t->sgl[slot], off, &t->ev[slot]);
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
            err = axl_dfs_wait(t, i);
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

    close(src_fd);

    /* Every write from offset 0 (or the resumed size) to off landed, so off is the file's
     * size, and AXL_Wait need not ask the servers for it again. */
    if (rc == AXL_SUCCESS && size != NULL) {
        if ((unsigned long long) off == *size) {
            *size_checked = 1;
        } else {
            AXL_ERR("file `%s' size is %llu expected %lu", dst_file, (unsigned long long) off, *size);
            rc = AXL_FAILURE;
        }
    }

    if (rc == AXL_SUCCESS) {
        axl_dfs_count(t->xfer, 0, bytes);
    } else {
        /* a failed write can leave a hole under a full-size file */
        err = dfs_remove(dfs, parent, name, false, NULL);
        if (err != 0) {
            AXL_ERR("Removing %s after a failed copy: %s", dst_file, strerror(err));
        }
    }
    return rc;
}
