#include <gdbm.h>

#include "redis.h"
#include "nds.h"

#include <stdio.h>

#define FREEZER_FILENAME_LEN 255

void freezer_filename(redisDb *db, char *buf) {
    snprintf(buf, FREEZER_FILENAME_LEN-1, "freezer_%i.gdbm", db->id);
}

robj *getNDS(redisDb *db, robj *key) {
    /* Perhaps it's in the freezer... */
    char freezer_file[FREEZER_FILENAME_LEN];
    GDBM_FILE freezer = NULL;
    datum frz_key, frz_val;
    rio payload;
    int type;
    robj *obj = NULL;
    
    redisLog(REDIS_DEBUG, "Looking up %s in NDS", (char *)key->ptr);
    freezer_filename(db, freezer_file);
    while (!freezer) {
        freezer = gdbm_open(freezer_file, 0, GDBM_READER, 0640, NULL);
        if (!freezer && gdbm_errno != GDBM_CANT_BE_READER) {
            redisLog(REDIS_WARNING, "Could not open %s: %s", freezer_file, gdbm_strerror(gdbm_errno));
            return NULL;
        }
    }
    
    frz_key.dptr  = (char *)key->ptr;
    frz_key.dsize = sdslen((char *)key->ptr);
    frz_val = gdbm_fetch(freezer, frz_key);
    
    if (frz_val.dptr) {
        redisLog(REDIS_DEBUG, "Key %s was found in NDS", (char *)key->ptr);
        /* We got one!  Thaw and return */
        
        /* Is the data valid? */
        if (verifyDumpPayload((unsigned char *)frz_val.dptr, (unsigned)frz_val.dsize) == REDIS_ERR) {
            redisLog(REDIS_ERR, "Invalid payload for key %s; ignoring", (char *)key->ptr);
            goto nds_cleanup;
        }
        
        rioInitWithBuffer(&payload, frz_val.dptr);
        if (((type = rdbLoadObjectType(&payload) == -1)) ||
            ((obj  = rdbLoadObject(type,&payload)) == NULL))
        {
            redisLog(REDIS_ERR, "Bad data format for key %s; ignoring", (char *)key->ptr);
            goto nds_cleanup;
        }
    }

nds_cleanup:
    if (frz_val.dptr) {
        free(frz_val.dptr);
    }
    gdbm_close(freezer);
    return obj;
}

void setNDS(redisDb *db, robj *key, robj *val) {
    char freezer_file[FREEZER_FILENAME_LEN];
    GDBM_FILE freezer = NULL;
    datum frz_key, frz_val;
    rio payload;
    
    /* We *can* end up in the situation where setNDS gets called on a key
     * that has been deleted.  Rather than try to special-case that
     * elsewhere, we'll just throw our hands in the air and return early if
     * that's the case.
     */
    if (!val) {
        return;
    }
        
    redisLog(REDIS_DEBUG, "Writing %s to NDS", (char *)key->ptr);
    freezer_filename(db, freezer_file);
    while (!freezer) {
        freezer = gdbm_open(freezer_file, 0, GDBM_WRCREAT, 0640, NULL);
        if (!freezer && gdbm_errno != GDBM_CANT_BE_WRITER) {
            redisLog(REDIS_WARNING, "Could not open %s: %s", freezer_file, gdbm_strerror(gdbm_errno));
            return;
        }
    }
        
    frz_key.dptr  = (char *)key->ptr;
    frz_key.dsize = sdslen((char *)key->ptr);
    
    createDumpPayload(&payload, val);
    frz_val.dptr  = (char *)payload.io.buffer.ptr;
    frz_val.dsize = sdslen((char *)payload.io.buffer.ptr);
    
    gdbm_store(freezer, frz_key, frz_val, GDBM_REPLACE);
    
    gdbm_close(freezer);
}

/* Delete a key from the NDS.  Returns 0 if the key wasn't found, or
 * 1 if it was.  -1 is returned on error.
 */
int delNDS(redisDb *db, robj *key) {
    char freezer_file[FREEZER_FILENAME_LEN];
    GDBM_FILE freezer = NULL;
    datum frz_key;
    int rv;
    
    redisLog(REDIS_DEBUG, "Deleting %s from NDS", (char *)key->ptr);
    freezer_filename(db, freezer_file);
    while (!freezer) {
        freezer = gdbm_open(freezer_file, 0, GDBM_WRCREAT, 0640, NULL);
        if (!freezer && gdbm_errno != GDBM_CANT_BE_WRITER) {
            redisLog(REDIS_WARNING, "Could not open %s: %s", freezer_file, gdbm_strerror(gdbm_errno));
            return -1;
        }
    }
    
    frz_key.dptr  = (char *)key->ptr;
    frz_key.dsize = sdslen((char *)key->ptr);
    
    rv = gdbm_delete(freezer, frz_key);
    
    redisLog(REDIS_DEBUG, "gdbm_delete returned %i", rv);
    
    gdbm_close(freezer);
    
    return !rv;
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
    for (int j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        dictIterator *di;
        dictEntry *deKey, *deVal;
        robj roKey;
        
        if (dictSize(db->dirty_keys) == 0) continue;
        
        di = dictGetSafeIterator(db->dirty_keys);
        if (!di) {
            return REDIS_ERR;
        }
        
        while ((deKey = dictNext(di)) != NULL) {
            sds keystr = dictGetKey(deKey);
            redisLog(REDIS_DEBUG, "Flushing key '%s' to NDS", keystr);
            deVal = dictFind(db->dict, keystr);
            if (!deVal) {
                /* Key must have been deleted after it got dirtied.  That's
                 * easy to handle -- it's already been deleted from the
                 * freezer, so we just ignore it now and everything goes
                 * away. */
                redisLog(REDIS_DEBUG, "Key '%s' was deleted; not saving", keystr);
                sdsfree(keystr);
                continue;
            }
            
            roKey.ptr = keystr;
            setNDS(db, &roKey, dictGetVal(deVal));
            sdsfree(keystr);
        }
    }
    
    server.dirty = 0;
    
    return REDIS_OK;
}

void backgroundNDSFlushDoneHandler(int exitcode, int bysignal) {
    redisLog(REDIS_NOTICE, "NDS background save completed.  exitcode=%i, bysignal=%i", exitcode, bysignal);
    for (int j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        dictEmpty(db->flushing_keys);
    }
    server.lastsave = time(NULL);
    server.nds_child_pid = -1;
    server.dirty -= server.dirty_before_bgsave;
}
