aof-replay tool
---------------

if you want to migrate your data from old cluster (maybe sharding by client libs like PRedis), you need to export data from old cluster and import to the newone.

1. it will hard if data is distributed at several instances in the old cluster. (can not just ``SLAVE OF``...)
2. online Migration (you can not stop writing)

I make a tool to replay the aof of old cluster::

    ./redis-cli -h 127.0.0.5 -p 22000 --replay /path/to/appendonly.aof
    ./redis-cli -h 127.0.0.5 -p 24000 --replay /path/to/appendonly.aof --filter kkk-1000 --orig kkk --rewrite xxxxxx


points:

1. remove all ``select`` / ``multi``
2. change ``MSET/MSETNX/DEL`` to many ``SET/SETNX/DEL`` cmd;
3. ``--filter`` : filter key by prefix
4. ``--orig``, ``--rewrite``, rewrite key.
5. follow aof modification like ``tail -f``
6. ``--check`` check any command not supported

this is for practice purpose
