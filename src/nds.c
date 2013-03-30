#include <gdbm.h>

#include "redis.h"
#include "nds.h"

robj *getNDS(redisDb *db, robj *key) {
    /* Perhaps it's in the freezer... */
    GDBM_FILE freezer;
    datum frz_key, frz_val;
    rio payload;
    int type;
    robj *obj = NULL;
    
    redisLog(REDIS_DEBUG, "Looking up %s in NDS", (char *)key->ptr);
    freezer = gdbm_open("freezer.gdbm", 0, GDBM_READER, 0640, NULL);
    if (!freezer) {
        redisLog(REDIS_WARNING, "Could not open freezer.gdbm");
        return NULL;
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
    GDBM_FILE freezer;
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
    freezer = gdbm_open("freezer.gdbm", 0, GDBM_WRCREAT, 0640, NULL);
    if (!freezer) {
        redisLog(REDIS_WARNING, "Could not open freezer.gdbm");
        return;
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
    GDBM_FILE freezer;
    datum frz_key;
    int rv;
    
    redisLog(REDIS_DEBUG, "Deleting %s from NDS", (char *)key->ptr);
    freezer = gdbm_open("freezer.gdbm", 0, GDBM_WRCREAT, 0640, NULL);
    if (!freezer) {
        redisLog(REDIS_WARNING, "Could not open freezer.gdbm");
        return -1;
    }
    
    frz_key.dptr  = (char *)key->ptr;
    frz_key.dsize = sdslen((char *)key->ptr);
    
    rv = gdbm_delete(freezer, frz_key);
    
    redisLog(REDIS_DEBUG, "gdbm_delete returned %i", rv);
    
    gdbm_close(freezer);
    
    return !rv;
}
