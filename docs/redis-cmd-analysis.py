#!/usr/bin/env python
#coding: utf-8
#file   : redis-cmd-analysis.py
#author : ning
#date   : 2014-03-11 16:33:32

NULL = None
noPreloadGetKeys = None
renameGetKeys = None
zunionInterGetKeys = None

'''
struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
    char *sflags; /* Flags as string representation, one char per flag. */
    int flags;    /* The actual flags, obtained from the 'sflags' field. */
    /* Use a function to determine keys arguments in a command line. */
    redisGetKeysProc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    long long microseconds, calls;
};

 * arity: number of arguments, it is possible to use -N to say >= N
 * first_key_index: first argument that is a key
 * last_key_index: last argument that is a key
 * key_step: step to get all the keys from first to last argument. For instance
 *           in MSET the step is two since arguments are key,val,key,val,...

'''


redis_command_table = [
    #name               ,proc ,arity, sflag,  flag, get_key            first last keystep microseconds, calls
    ("GET"              ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SET"              ,NULL , -3 , "wm"    , 0 , noPreloadGetKeys   , 1 , 1  , 1 , 0 , 0) ,
    ("SETNX"            ,NULL , 3  , "wm"    , 0 , noPreloadGetKeys   , 1 , 1  , 1 , 0 , 0) ,
    ("SETEX"            ,NULL , 4  , "wm"    , 0 , noPreloadGetKeys   , 1 , 1  , 1 , 0 , 0) ,
    ("PSETEX"           ,NULL , 4  , "wm"    , 0 , noPreloadGetKeys   , 1 , 1  , 1 , 0 , 0) ,
    ("APPEND"           ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("STRLEN"           ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("DEL"              ,NULL , -2 , "w"     , 0 , noPreloadGetKeys   , 1 , -1 , 1 , 0 , 0) ,
    ("EXISTS"           ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SETBIT"           ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("GETBIT"           ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SETRANGE"         ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("GETRANGE"         ,NULL , 4  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SUBSTR"           ,NULL , 4  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("INCR"             ,NULL , 2  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("DECR"             ,NULL , 2  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("MGET"             ,NULL , -2 , "r"     , 0 , NULL               , 1 , -1 , 1 , 0 , 0) ,
    ("RPUSH"            ,NULL , -3 , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LPUSH"            ,NULL , -3 , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("RPUSHX"           ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LPUSHX"           ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LINSERT"          ,NULL , 5  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("RPOP"             ,NULL , 2  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LPOP"             ,NULL , 2  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("BRPOP"            ,NULL , -3 , "ws"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("BRPOPLPUSH"       ,NULL , 4  , "wms"   , 0 , NULL               , 1 , 2  , 1 , 0 , 0) ,
    ("BLPOP"            ,NULL , -3 , "ws"    , 0 , NULL               , 1 , -2 , 1 , 0 , 0) ,
    ("LLEN"             ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LINDEX"           ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LSET"             ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LRANGE"           ,NULL , 4  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LTRIM"            ,NULL , 4  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("LREM"             ,NULL , 4  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("RPOPLPUSH"        ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 2  , 1 , 0 , 0) ,
    ("SADD"             ,NULL , -3 , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SREM"             ,NULL , -3 , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SMOVE"            ,NULL , 4  , "w"     , 0 , NULL               , 1 , 2  , 1 , 0 , 0) ,
    ("SISMEMBER"        ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SCARD"            ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SPOP"             ,NULL , 2  , "wRs"   , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SRANDMEMBER"      ,NULL , -2 , "rR"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SINTER"           ,NULL , -2 , "rS"    , 0 , NULL               , 1 , -1 , 1 , 0 , 0) ,
    ("SINTERSTORE"      ,NULL , -3 , "wm"    , 0 , NULL               , 1 , -1 , 1 , 0 , 0) ,
    ("SUNION"           ,NULL , -2 , "rS"    , 0 , NULL               , 1 , -1 , 1 , 0 , 0) ,
    ("SUNIONSTORE"      ,NULL , -3 , "wm"    , 0 , NULL               , 1 , -1 , 1 , 0 , 0) ,
    ("SDIFF"            ,NULL , -2 , "rS"    , 0 , NULL               , 1 , -1 , 1 , 0 , 0) ,
    ("SDIFFSTORE"       ,NULL , -3 , "wm"    , 0 , NULL               , 1 , -1 , 1 , 0 , 0) ,
    ("SMEMBERS"         ,NULL , 2  , "rS"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SSCAN"            ,NULL , -3 , "rR"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZADD"             ,NULL , -4 , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZINCRBY"          ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZREM"             ,NULL , -3 , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZREMRANGEBYSCORE" ,NULL , 4  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZREMRANGEBYRANK"  ,NULL , 4  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZUNIONSTORE"      ,NULL , -4 , "wm"    , 0 , zunionInterGetKeys , 0 , 0  , 0 , 0 , 0) ,
    ("ZINTERSTORE"      ,NULL , -4 , "wm"    , 0 , zunionInterGetKeys , 0 , 0  , 0 , 0 , 0) ,
    ("ZRANGE"           ,NULL , -4 , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZRANGEBYSCORE"    ,NULL , -4 , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZREVRANGEBYSCORE" ,NULL , -4 , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZCOUNT"           ,NULL , 4  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZREVRANGE"        ,NULL , -4 , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZCARD"            ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZSCORE"           ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZRANK"            ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZREVRANK"         ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("ZSCAN"            ,NULL , -3 , "rR"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HSET"             ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HSETNX"           ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HGET"             ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HMSET"            ,NULL , -4 , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HMGET"            ,NULL , -3 , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HINCRBY"          ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HINCRBYFLOAT"     ,NULL , 4  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HDEL"             ,NULL , -3 , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HLEN"             ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HKEYS"            ,NULL , 2  , "rS"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HVALS"            ,NULL , 2  , "rS"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HGETALL"          ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HEXISTS"          ,NULL , 3  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("HSCAN"            ,NULL , -3 , "rR"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("INCRBY"           ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("DECRBY"           ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("INCRBYFLOAT"      ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("GETSET"           ,NULL , 3  , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("MSET"             ,NULL , -3 , "wm"    , 0 , NULL               , 1 , -1 , 2 , 0 , 0) ,
    ("MSETNX"           ,NULL , -3 , "wm"    , 0 , NULL               , 1 , -1 , 2 , 0 , 0) ,
    ("RANDOMKEY"        ,NULL , 1  , "rR"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SELECT"           ,NULL , 2  , "rl"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("MOVE"             ,NULL , 3  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("RENAME"           ,NULL , 3  , "w"     , 0 , renameGetKeys      , 1 , 2  , 1 , 0 , 0) ,
    ("RENAMENX"         ,NULL , 3  , "w"     , 0 , renameGetKeys      , 1 , 2  , 1 , 0 , 0) ,
    ("EXPIRE"           ,NULL , 3  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("EXPIREAT"         ,NULL , 3  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("PEXPIRE"          ,NULL , 3  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("PEXPIREAT"        ,NULL , 3  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("KEYS"             ,NULL , 2  , "rS"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SCAN"             ,NULL , -2 , "rR"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("DBSIZE"           ,NULL , 1  , "r"     , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("AUTH"             ,NULL , 2  , "rslt"  , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("PING"             ,NULL , 1  , "rt"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("ECHO"             ,NULL , 2  , "r"     , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SAVE"             ,NULL , 1  , "ars"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("BGSAVE"           ,NULL , 1  , "ar"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("BGREWRITEAOF"     ,NULL , 1  , "ar"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SHUTDOWN"         ,NULL , -1 , "arl"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("LASTSAVE"         ,NULL , 1  , "rR"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("TYPE"             ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("MULTI"            ,NULL , 1  , "rs"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("EXEC"             ,NULL , 1  , "sM"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("DISCARD"          ,NULL , 1  , "rs"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SYNC"             ,NULL , 1  , "ars"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("PSYNC"            ,NULL , 3  , "ars"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("REPLCONF"         ,NULL , -1 , "arslt" , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("FLUSHDB"          ,NULL , 1  , "w"     , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("FLUSHALL"         ,NULL , 1  , "w"     , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SORT"             ,NULL , -2 , "wm"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("INFO"             ,NULL , -1 , "rlt"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("MONITOR"          ,NULL , 1  , "ars"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("TTL"              ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("PTTL"             ,NULL , 2  , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("PERSIST"          ,NULL , 2  , "w"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("SLAVEOF"          ,NULL , 3  , "ast"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("DEBUG"            ,NULL , -2 , "as"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("CONFIG"           ,NULL , -2 , "ar"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SUBSCRIBE"        ,NULL , -2 , "rpslt" , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("UNSUBSCRIBE"      ,NULL , -1 , "rpslt" , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("PSUBSCRIBE"       ,NULL , -2 , "rpslt" , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("PUNSUBSCRIBE"     ,NULL , -1 , "rpslt" , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("PUBLISH"          ,NULL , 3  , "pltr"  , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("PUBSUB"           ,NULL , -2 , "pltrR" , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("WATCH"            ,NULL , -2 , "rs"    , 0 , noPreloadGetKeys   , 1 , -1 , 1 , 0 , 0) ,
    ("UNWATCH"          ,NULL , 1  , "rs"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("RESTORE"          ,NULL , 4  , "awm"   , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("MIGRATE"          ,NULL , 6  , "aw"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("DUMP"             ,NULL , 2  , "ar"    , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
    ("OBJECT"           ,NULL , -2 , "r"     , 0 , NULL               , 2 , 2  , 2 , 0 , 0) ,
    ("CLIENT"           ,NULL , -2 , "ar"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("EVAL"             ,NULL , -3 , "s"     , 0 , zunionInterGetKeys , 0 , 0  , 0 , 0 , 0) ,
    ("EVALSHA"          ,NULL , -3 , "s"     , 0 , zunionInterGetKeys , 0 , 0  , 0 , 0 , 0) ,
    ("SLOWLOG"          ,NULL , -2 , "r"     , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("SCRIPT"           ,NULL , -2 , "ras"   , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("TIME"             ,NULL , 1  , "rR"    , 0 , NULL               , 0 , 0  , 0 , 0 , 0) ,
    ("BITOP"            ,NULL , -4 , "wm"    , 0 , NULL               , 2 , -1 , 1 , 0 , 0) ,
    ("BITCOUNT"         ,NULL , -2 , "r"     , 0 , NULL               , 1 , 1  , 1 , 0 , 0) ,
]

twemproxy_command_table = [
('SCRIPTEXISTS','No'), #redis do not has this cmd
('SCRIPTFLUSH','No'),
('DEL','Yes'),
('DUMP','Yes'),
('EXISTS','Yes'),
('EXPIRE','Yes'),
('EXPIREAT','Yes'),
('KEYS','No'),
('MIGRATE','No'),
('MOVE','No'),
('OBJECT','No'),
('PERSIST','Yes'),
('PEXPIRE','Yes'),
('PEXPIREAT','Yes'),
('PTTL','Yes'),
('RANDOMKEY','No'),
('RENAME','No'),
('RENAMENX','No'),
('RESTORE','Yes'),
('SORT','No'),
('TTL','Yes'),
('TYPE','Yes'),
('APPEND','Yes'),
('BITCOUNT','Yes'),
('BITOP','No'),
('DECR','Yes'),
('DECRBY','Yes'),
('GET','Yes'),
('GETBIT','Yes'),
('GETRANGE','Yes'),
('GETSET','Yes'),
('INCR','Yes'),
('INCRBY','Yes'),
('INCRBYFLOAT','Yes'),
('MGET','Yes'),
('MSET','No'),
('MSETNX','No'),
('PSETEX','Yes'),
('SET','Yes'),
('SETBIT','Yes'),
('SETEX','Yes'),
('SETNX','Yes'),
('SETRANGE','Yes'),
('STRLEN','Yes'),
('HDEL','Yes'),
('HEXISTS','Yes'),
('HGET','Yes'),
('HGETALL','Yes'),
('HINCRBY','Yes'),
('HINCRBYFLOAT','Yes'),
('HKEYS','Yes'),
('HLEN','Yes'),
('HMGET','Yes'),
('HMSET','Yes'),
('HSET','Yes'),
('HSETNX','Yes'),
('HVALS','Yes'),
('BLPOP','No'),
('BRPOP','No'),
('BRPOPLPUSH','No'),
('LINDEX','Yes'),
('LINSERT','Yes'),
('LLEN','Yes'),
('LPOP','Yes'),
('LPUSH','Yes'),
('LPUSHX','Yes'),
('LRANGE','Yes'),
('LREM','Yes'),
('LSET','Yes'),
('LTRIM','Yes'),
('RPOP','Yes'),
('RPOPLPUSH','Yes*'),
('RPUSH','Yes'),
('RPUSHX','Yes'),
('SADD','Yes'),
('SCARD','Yes'),
('SDIFF','Yes*'),
('SDIFFSTORE','Yes*'),
('SINTER','Yes*'),
('SINTERSTORE','Yes*'),
('SISMEMBER','Yes'),
('SMEMBERS','Yes'),
('SMOVE','Yes*'),
('SPOP','Yes'),
('SRANDMEMBER','Yes'),
('SREM','Yes'),
('SUNION','Yes*'),
('SUNIONSTORE','Yes*'),
('ZADD','Yes'),
('ZCARD','Yes'),
('ZCOUNT','Yes'),
('ZINCRBY','Yes'),
('ZINTERSTORE','Yes*'),
('ZRANGE','Yes'),
('ZRANGEBYSCORE','Yes'),
('ZRANK','Yes'),
('ZREM','Yes'),
('ZREMRANGEBYRANK','Yes'),
('ZREMRANGEBYSCORE','Yes'),
('ZREVRANGE','Yes'),
('ZREVRANGEBYSCORE','Yes'),
('ZREVRANK','Yes'),
('ZSCORE','Yes'),
('ZUNIONSTORE','Yes*'),
('PSUBSCRIBE','No'),
('PUBLISH','No'),
('PUNSUBSCRIBE','No'),
('SUBSCRIBE','No'),
('UNSUBSCRIBE','No'),
('DISCARD','No'),
('EXEC','No'),
('MULTI','No'),
('UNWATCH','No'),
('WATCH','No'),
('EVAL','Yes*'),
('EVALSHA','Yes*'),
('SCRIPTKILL','No'),
('SCRIPTLOAD','No'),
('AUTH','No'),
('ECHO','No'),
('PING','No'),
('QUIT','No'),
('SELECT','No'),
('BGREWRITEAOF','No'),
('BGSAVE','No'),
('CLIENTKILL','No'),
('CLIENTLIST','No'),
('CONFIGGET','No'),
('CONFIGSET','No'),
('CONFIGRESETSTAT','No'),
('DBSIZE','No'),
('DEBUGOBJECT','No'),
('DEBUGSEGFAULT','No'),
('FLUSHALL','No'),
('FLUSHDB','No'),
('INFO','No'),
('LASTSAVE','No'),
('MONITOR','No'),
('SAVE','No'),
('SHUTDOWN','No'),
('SLAVEOF','No'),
('SLOWLOG','No'),
('SYNC','No'),
('TIME','No'),
]


def two_table_diff():
    all_cmd = redis_command_table + twemproxy_command_table
    all_cmd = [c[0] for c in all_cmd]

    cmds = {}
    for key in all_cmd:
        cmds[key] = [0, 0]

    for cmd in redis_command_table:
        name, proc, arity, sflag, flag, get_key, first, last, keystep, microseconds, calls = cmd
        #无key的不支持.
        if 0 == first:
            cmds[name][0] = 'No'
        else:
            cmds[name][0] = 'Yes'

    for name, support in twemproxy_command_table:
        cmds[name][1] = support

    print '两个列表的不同:'
    for k, v in cmds.items():
        if v[0] != v[1]:
            print k, v

    print 'twemproxy说支持的, redis列表中, 应该都支持'
    for k, v in cmds.items():
        if 'Yes' == v[1] and 'Yes' != v[0]:
            print k, v

    print 'twemproxy说支持的, 应该都只有一个key, 并且key是在第一个位置, 除了MGET/DEL'

    def assert_key_first_last(input_name):
        for cmd in redis_command_table:
            name, proc, arity, sflag, flag, get_key, first, last, keystep, microseconds, calls = cmd
            if input_name == name:
                #print name, first, last
                assert first == last == 1
                return
        assert False

    for k, v in cmds.items():
        if k in ['DEL', 'MGET']:
            continue
        if 'Yes' == v[1]:
            assert_key_first_last(k)


    print 'redis列表中, 有多少种last值, 每种的支持情况:'
    for cmd in redis_command_table:
        name, proc, arity, sflag, flag, get_key, first, last, keystep, microseconds, calls = cmd
        if last == 0:
            assert(first==0)
        elif last == 1:
            assert(first==1)
        else:
            print last, name, cmds[name]


'''
redis列表中, 有多少种last值, 每种的支持情况:
-1 DEL ['Yes', 'Yes']
-1 MSET ['Yes', 'No']
-1 MSETNX ['Yes', 'No']

-1 MGET ['Yes', 'Yes']  #读, 没关系
-1 WATCH ['Yes', 'No']  #WATCH明确不支持

-1 BITOP ['Yes', 'No']  #BITOP AND dest key1 key2 明确不支持.
-2 BLPOP ['Yes', 'No']  #明确不支持
2 RENAME ['Yes', 'No']  #下面2个参数的, 明确不支持
2 RENAMENX ['Yes', 'No']
2 BRPOPLPUSH ['Yes', 'No']
'''

if __name__ == "__main__":
    two_table_diff()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

