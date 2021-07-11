import os
# import psutil
import time

from udb_py import *


# process = psutil.Process(os.getpid())
start_time = None


def start():
    global start_time

    start_time = time.time()


def stop(samples=None, title=None, show_mem_usage=None):
    global start_time

    for_start_time = time.time()

    for _ in range(samples):
        pass

    for_total = time.time() - for_start_time

    if start_time:
        total = time.time() - start_time - for_total

        if samples:
            if title:
                print(title.upper())

            print("Total time: {} sec., per sample: {} sec., samples per second: {}, total samples: {}".format(
                total,
                total / samples,
                1 / total * samples,
                samples,
            ))

            # if show_mem_usage:
            #     print('{} MiB mem used'.format(process.memory_info().rss / (1024 * 1024)))
        else:
            print("Total time: {} sec.".format(total))

        print()


SAMPLES = 10000


udb = Udb({'a': UdbTextIndex(['a'])})

start()

for i in range(0, SAMPLES):
    udb.insert({'a': str(i)})

stop(SAMPLES, 'insert (full text, 1st index covers 1 field)', True)


start()

for i in range(0, SAMPLES):
    list(udb.select({'a': {'$text': str(i)}}))

stop(SAMPLES, 'select (full text, 1st index covers 1 field)')


# udb = Udb({'a': UdbBtreeIndex(['a'])})

# start()

# for i in range(0, SAMPLES):
#     udb.insert({'a': i})

# stop(SAMPLES, 'insert (btree, 1st index covers 1 field)', True)


# start()

# for i in range(0, SAMPLES):
#     list(udb.select({'a': i}))

# stop(SAMPLES, 'select (btree, 1st index covers 1 field)')


# start()

# for i in range(0, SAMPLES):
#     list(udb.select({'a': {'$gte': i, '$lt': i + 5}}))

# stop(SAMPLES, 'select (btree, 1st index covers 1 field, range scan - 5 records)')


# udb = Udb({'a': UdbBtreeIndex(['a']), 'b': UdbBtreeIndex(['b']), 'ab': UdbBtreeIndex(['a', 'b'])})

# start()

# for i in range(0, SAMPLES):
#     udb.insert({'a': i, 'b': i + 1})

# stop(SAMPLES, 'insert (btree, 1st index covers 1 field, 2nd index covers 1 field, 3rd index covers 2 fields)', True)


# start()

# for i in range(0, SAMPLES):
#     list(udb.select({'a': i}))

# stop(SAMPLES, 'select (btree, 1st index covers 1 field, 2nd index covers 1 field, 3rd index covers 2 fields)')


# udb = Udb({'a': UdbHashIndex(['a'])})

# start()

# for i in range(0, SAMPLES):
#     udb.insert({'a': i})

# stop(SAMPLES, 'insert (hash, 1st index covers 1 field)', True)


# start()

# for i in range(0, SAMPLES):
#     list(udb.select({'a': i}))

# stop(SAMPLES, 'select (hash, 1st index covers 1 field)')


# udb = Udb({'a': UdbHashIndex(['a']), 'b': UdbHashIndex(['b']), 'ab': UdbHashIndex(['a', 'b'])})

# start()

# for i in range(0, SAMPLES):
#     udb.insert({'a': i, 'b': i + 1})

# stop(SAMPLES, 'insert (hash, 1st index covers 1 field, 2nd index covers 1 field, 3rd index covers 2 fields)', True)


# start()

# for i in range(0, SAMPLES):
#     list(udb.select({'a': i}))

# stop(SAMPLES, 'select (hash, 1st index covers 1 field, 2nd index covers 1 field, 3rd index covers 2 fields)')


# udb = Udb({'a': UdbRtreeIndex('a')})

# start()

# for i in range(0, 100000):
#     udb.insert({'a': [i, i + 1]})

# stop(100000, 'insert (rtree, 1st index covers 1 field)', True)


# start()

# for i in range(0, 100000):
#     list(udb.select({'a': {'$near': {'x': i, 'y': i + 1, 'maxDistance': 2}}}, limit=5))

# stop(100000, 'select (rtree, 1st index covers 1 field, limit = 5)')
