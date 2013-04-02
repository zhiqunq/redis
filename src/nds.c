#include "redis.h"
#include "nds.h"

#include <leveldb/c.h>

#include <stdio.h>

#define FREEZER_FILENAME_LEN 255

/* Generate the name of the freezer we want, based on the database passed
 * in, and stuff the name into buf */
static void freezer_filename(redisDb *db, char *buf) {
    snprintf(buf, FREEZER_FILENAME_LEN-1, "freezer_%i", db->id);
}

/* Hide the insane troll logic required to open a leveldb.  Pass in the
 * redis DB to open the freezer for, and pass back a leveldb_t ready for
 * use.  Return NULL on failure (errors will be reported via redisLog).  */
static leveldb_t *nds_open(redisDb *db) {
    leveldb_options_t *opts = leveldb_options_create();
    char *err = NULL, freezer_name[FREEZER_FILENAME_LEN];
    leveldb_t *ldb;
    
    leveldb_options_set_create_if_missing(opts, 't');
    
    freezer_filename(db, freezer_name);
    
    ldb = leveldb_open(opts, freezer_name, &err);
    
    while (err) {
        redisLog(REDIS_WARNING, "Failed to open the freezer: %s", err);
        free(err);
        ldb = NULL;
        if (errno == EAGAIN) {
            ldb = leveldb_open(opts, freezer_name, &err);
        } else {
            goto cleanup;
        }
    }
    
cleanup:
    leveldb_options_destroy(opts);
    return ldb;    
}

/* Close a leveldb database.  No insane troll logic to be found, it's just
 * nice to have a consistent API. */
static void nds_close(leveldb_t *ldb) {
    if (ldb) {
        leveldb_close(ldb);
    }
}

/* Hide the insane troll logic required to get a value out of a leveldb. 
 * Pass in the DB and key to get the value for, and return an sds containing
 * the value if found, or NULL on error or key-not-found.  Will report
 * errors via redisLog.  */
static sds nds_get(redisDb *db, sds key) {
    leveldb_readoptions_t *opts = leveldb_readoptions_create();
    leveldb_t *ldb = nds_open(db);
    char *err = NULL, *valdata;
    size_t vallen;
    sds val = NULL;

    if (!ldb) {
        goto cleanup;
    }
    
    valdata = leveldb_get(ldb, opts, key, sdslen(key), &vallen, &err);
    
    if (err) {
        redisLog(REDIS_ERR, "Error while attempting to get value of %s: %s", key, err);
        free(err);
        valdata = NULL;
        goto cleanup;
    }
    
    if (valdata) {
        val = sdsnewlen(valdata, vallen);
        free(valdata);
    }

cleanup:
    nds_close(ldb);
    leveldb_readoptions_destroy(opts);
    return val;
}

/* Insane troll logic, put style.  Takes a redisDB, a key, and a value, and makes
 * sure they get into the database (or you at least know what's going on via the
 * logs). */
static void nds_put(redisDb *db, sds key, sds val) {
    leveldb_writeoptions_t *opts = leveldb_writeoptions_create();
    leveldb_t *ldb = nds_open(db);
    char *err = NULL;
    
    if (!ldb) {
        goto cleanup;
    }
    
    leveldb_put(ldb, opts, key, sdslen(key), val, sdslen(val), &err);
    
    if (err) {
        redisLog(REDIS_ERR, "Failed to put %s: %s", key, err);
        free(err);
        goto cleanup;
    }

cleanup:
    nds_close(ldb);
    leveldb_writeoptions_destroy(opts);
}

/* Deletion time!  Takes a redisDB and a key, and make the key go away.  Tells the user
 * about problems via the logs, and returns -1 if an error occured. */
static int nds_del(redisDb *db, sds key) {
    leveldb_writeoptions_t *opts = leveldb_writeoptions_create();
    leveldb_t *ldb = nds_open(db);
    int rv = 0;
    char *err = NULL;
    
    if (!ldb) {
        goto cleanup;
    }
    
    leveldb_delete(ldb, opts, key, sdslen(key), &err);
    
    if (err) {
        redisLog(REDIS_ERR, "Failed to delete %s: %s", key, err);
        free(err);
        rv = -1;
        goto cleanup;
    }

cleanup:
    nds_close(ldb);
    leveldb_writeoptions_destroy(opts);
    return rv;
}

/* Spam lots of puts in bulk.  Useful for flushes.  Returns the number of records
 * written on success, or -1 on error.  If error, there are no guarantees of which
 * keys may or may not have been written. */
static int nds_bulk_put(redisDb *db, sds *keys, sds *vals, int kcount) {
    leveldb_writeoptions_t *opts = leveldb_writeoptions_create();
    leveldb_writebatch_t *batch = leveldb_writebatch_create();
    char *err = NULL;
    leveldb_t *ldb;
    int rv = kcount;
    
    for (int i = 0; i < kcount; i++) {
        leveldb_writebatch_put(batch, keys[i], sdslen(keys[i]),
                                      vals[i], sdslen(vals[i])
                              );
    }
    
    ldb = nds_open(db);
    
    if (!ldb) {
        rv = kcount;
        goto cleanup;
    }
    leveldb_write(ldb, opts, batch, &err);

    if (err) {
        redisLog(REDIS_WARNING, "NDS batch write failed: %s", err);
        rv = -1;
        free(err);
        goto cleanup;
    }
    
cleanup:
    nds_close(ldb);
    leveldb_writeoptions_destroy(opts);
    leveldb_writebatch_destroy(batch);
    return rv;
}

robj *getNDS(redisDb *db, robj *key) {
    sds val;
    rio payload;
    int type;
    robj *obj = NULL;
    
    redisLog(REDIS_DEBUG, "Looking up %s in NDS", (char *)key->ptr);

    val = nds_get(db, key->ptr);
    
    if (val) {
        redisLog(REDIS_DEBUG, "Key %s was found in NDS", (char *)key->ptr);
        /* We got one!  Thaw and return */
        
        /* Is the data valid? */
        if (verifyDumpPayload((unsigned char *)val, (size_t)sdslen(val)) == REDIS_ERR) {
            redisLog(REDIS_ERR, "Invalid payload for key %s; ignoring", (char *)key->ptr);
            goto nds_cleanup;
        }
        
        rioInitWithBuffer(&payload, val);
        if (((type = rdbLoadObjectType(&payload) == -1)) ||
            ((obj  = rdbLoadObject(type,&payload)) == NULL))
        {
            redisLog(REDIS_ERR, "Bad data format for key %s; ignoring", (char *)key->ptr);
            goto nds_cleanup;
        }
    }

nds_cleanup:
    return obj;
}

void setNDS(redisDb *db, robj *key, robj *val) {
    rio payload;
    
    /* We *can* end up in the situation where setNDS gets called on a key
     * that has been deleted.  Rather than try to special-case that
     * elsewhere (by checking what we get out of lookupKey(), we'll just
     * throw our hands in the air and return early if that's the case.
     */
    if (!val) {
        return;
    }
        
    redisLog(REDIS_DEBUG, "Writing %s to NDS", (char *)key->ptr);
    
    createDumpPayload(&payload, val);
    nds_put(db, key->ptr, payload.io.buffer.ptr);
}

/* Delete a key from the NDS.  Returns 0 if the key wasn't found, or
 * 1 if it was.  -1 is returned on error.
 */
int delNDS(redisDb *db, robj *key) {
    sds val;
    int rv = 0;
    
    redisLog(REDIS_DEBUG, "Deleting %s from NDS", (char *)key->ptr);
    
    /* This is a bit racey, but leveldb doesn't appear to give me any way to
     * find out directly from the API whether or not a key was actually deleted.
     */
    val = nds_get(db, key->ptr);
    if (val) {
        sdsfree(val);
        rv = 1;
    }
    
    if (nds_del(db, key->ptr) < 0) {
        rv = -1;
    }

    return rv;
}

/* Add the key to the dirty keys list if it isn't there already */
void touchDirtyKey(redisDb *db, sds sdskey) {
    sds copy = sdsdup(sdskey);
    dictEntry *de = dictFind(db->dirty_keys, copy);
    
    if (!de) {
        dictAdd(db->dirty_keys, copy, NULL);
    }
}

int isDirtyKey(redisDb *db, sds sdskey) {
    dictEntry *de = dictFind(db->dirty_keys, sdskey);
    
    if (de) {
        return 1;
    }
    
    de = dictFind(db->flushing_keys, sdskey);
    
    if (de) {
        return 1;
    }
    
    return 0;
}
    
/* Fork and flush all the dirty keys out to disk. */
int backgroundDirtyKeysFlush() {
    pid_t childpid;

    if (server.nds_child_pid != -1) return REDIS_ERR;
    
    server.dirty_before_bgsave = server.dirty;

    if ((childpid = fork()) == 0) {
        int retval;

        redisLog(REDIS_DEBUG, "In child");
        
        /* Child */
        if (server.ipfd > 0) close(server.ipfd);
        if (server.sofd > 0) close(server.sofd);
        
        redisSetProcTitle("redis-nds-flush");
        
        retval = flushDirtyKeys();
        
        exitFromChild((retval == REDIS_OK) ? 0 : 1);
    } else {
        /* Parent */
        if (childpid == -1) {
            redisLog(REDIS_WARNING, "Can't save in background: fork: %s",
                     strerror(errno));
            return REDIS_ERR;
        }
        
        redisLog(REDIS_NOTICE, "Dirty key flush started in PID %d", childpid);
        server.nds_child_pid = childpid;
        /* Rotate the dirty keys into the flushing keys list, and use the
         * previous flushing keys list as the new dirty keys list. */
        for (int j = 0; j < server.dbnum; j++) {
            redisDb *db = server.db+j;
            dict *dTmp;
            dTmp = db->flushing_keys;
            db->flushing_keys = db->dirty_keys;
            db->dirty_keys = dTmp;
            dictEmpty(db->dirty_keys);
        }
        return REDIS_OK;
    }
    
    /* Can't happen */
    return REDIS_ERR;
}

int flushDirtyKeys() {
    redisLog(REDIS_DEBUG, "Flushing dirty keys");
    for (int j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        dictIterator *di;
        dictEntry *deKey, *deVal;
        sds *keys;
        sds *vals;
        int nkeys = dictSize(db->dirty_keys);
        int i = 0;
        rio payload;
        
        if (nkeys == 0) continue;

        redisLog(REDIS_NOTICE, "Planning on flushing up to %i keys to disk for DB %i", nkeys, db->id);

        keys = zmalloc(nkeys * sizeof(sds));
        if (!keys) {
            return REDIS_ERR;
        }
        vals = zmalloc(nkeys * sizeof(sds));
        if (!vals) {
            zfree(keys);
            return REDIS_ERR;
        }
        
        di = dictGetSafeIterator(db->dirty_keys);
        if (!di) {
            redisLog(REDIS_WARNING, "dictGetSafeIterator failed");
            zfree(keys);
            zfree(vals);
            return REDIS_ERR;
        }
        
        while ((deKey = dictNext(di)) != NULL) {
            rio payload;
            sds keystr = dictGetKey(deKey);
            deVal = dictFind(db->dict, keystr);
            if (!deVal) {
                /* Key must have been deleted after it got dirtied.  That's
                 * easy to handle -- it's already been deleted from the
                 * freezer, so we just ignore it now and everything goes
                 * away. */
                redisLog(REDIS_DEBUG, "Key '%s' was deleted; not saving", keystr);
                nkeys--;
                continue;
            }
            
            createDumpPayload(&payload, dictGetVal(deVal));
            keys[i] = keystr;
            vals[i] = payload.io.buffer.ptr;
            i++;
        }

        i = nds_bulk_put(db, keys, vals, nkeys);
        
        if (nkeys == i) {
            redisLog(REDIS_NOTICE, "Flushed %i keys for DB %i", i, db->id);
        } else {
            redisLog(REDIS_WARNING, "Can't happen: flushed a short batch of keys (expected %i, actually flushed %i)", nkeys, i);
        }
        
        /* Cleanup */
        zfree(keys);
        zfree(vals);
    }
    
    server.dirty = 0;

    redisLog(REDIS_DEBUG, "Flush complete");
    return REDIS_OK;
}

void backgroundNDSFlushDoneHandler(int exitcode, int bysignal) {
    redisLog(REDIS_NOTICE, "NDS background save completed.  exitcode=%i, bysignal=%i", exitcode, bysignal);
    if (exitcode == 0 || bysignal == 0) {
        for (int j = 0; j < server.dbnum; j++) {
            redisDb *db = server.db+j;
            dictEmpty(db->flushing_keys);
            server.dirty -= server.dirty_before_bgsave;
        }
    }
    server.lastsave = time(NULL);
    server.nds_child_pid = -1;
}
