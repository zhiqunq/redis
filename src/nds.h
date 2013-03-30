#ifndef NDS_H
#define NDS_H

#include <stdlib.h>

#include "redis.h"

robj *getNDS(redisDb *db, robj *key);
void  setNDS(redisDb *db, robj *key, robj *val);
int   delNDS(redisDb *db, robj *key);
#endif
