#ifndef NDS_H
#define NDS_H

#include <stdlib.h>

#include "redis.h"

robj *getNDS(redisDb *db, robj *key);
void  setNDS(redisDb *db, robj *key, robj *val);
int   delNDS(redisDb *db, robj *key);
void  nukeNDSFromOrbit();

void  touchDirtyKey(redisDb *db, sds sdskey);
int   isDirtyKey(redisDb *db, sds sdskey);

int   backgroundDirtyKeysFlush();
int   flushDirtyKeys();
void  backgroundNDSFlushDoneHandler();
#endif
