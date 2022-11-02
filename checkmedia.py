#!/usr/bin/python -uROO

import sqlite3
from sys import argv, exit
import os
import fnmatch
import hashlib
import time
import threading
import queue
import multiprocessing

tlock = threading.Lock()
tqueue = queue.Queue()
NUMTHREADS = int(multiprocessing.cpu_count() / 2)
if NUMTHREADS <= 1:
    NUMTHREADS += 1
RSIZE = 1024 * 1024


def logger(msg):
    tlock.acquire()
    print(msg)
    tlock.release()
    return


def getFileList(rundir):
    try:
        filelist = []
        for root, dir, files in os.walk(rundir):
            for items in fnmatch.filter(files, "*"):
                full = root + os.sep + items.strip()
                filelist.append(full)
    except Exception as ex:
        logger("Unable to get directory listing: %s" % str(ex))
        exit(-1)

    return sorted(filelist)


def openDB(dbfile="/var/tmp/media.db"):
    try:
        conn = sqlite3.connect(dbfile, check_same_thread=False)
        cur = conn.cursor()
        conn.text_factory = str
        cur.execute("PRAGMA auto_vacuum = 2;")
        cur.execute("PRAGMA encoding = 'UTF-8';")
        cur.execute("PRAGMA temp_store = MEMORY;")
        cur.execute("PRAGMA journal_mode = MEMORY;")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS media (path TEXT NOT NULL, md5 TEXT NOT NULL, atime UNSIGNED INT NOT NULL, mtime UNSINGED INT NOT NULL, ctime UNSIGNED INT NOT NULL, size UNSIGNED INT NOT NULL, whent DATETIME default current_timestamp);"
        )
    except Exception as ex:
        logger("Unable to open media DB: %s" % str(ex))
        exit(-2)
    return (conn, cur)


def closeDB(conn, cur):
    try:
        conn.commit()
        cur.execute("VACUUM;")
        conn.commit()
        cur.execute("CREATE INDEX IF NOT EXISTS pathidx ON media(path);")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as ex:
        logger("Unable to close DB: %s" % str(ex))
        exit(-3)
    return


def insertDB(cur, path, fhash, atime, mtime, ctime, size):
    tlock.acquire()
    try:
        cur.execute(
            "INSERT INTO media VALUES(?, ?, ?, ?, ?, ?, current_timestamp);",
            (str(path), str(fhash), atime, mtime, ctime, size),
        )
        tqueue.put(path)
    except Exception as ex:
        tlock.release()
        logger("Unable to insert %s into media DB: %s" % (str(path), str(ex)))
        return
    tlock.release()
    return


def getDB(cur, path):
    tlock.acquire()
    try:
        cur.execute(
            "SELECT md5, atime, mtime, ctime, size, whent FROM media WHERE path=? ORDER BY whent DESC LIMIT 1;",
            (str(path),),
        )
        results = cur.fetchone()
    except Exception as ex:
        tlock.release()
        logger("Unable to get data from media DB: %s" % str(ex))
        return
    tlock.release()
    if results is None or len(results) < 5:
        return [None, None, None, None, None, None]
    try:
        rtime = time.strptime(str(results[5]), "%Y-%m-%d %H:%M:%S")
    except Exception as ex:
        logger("Unable to convert DB time %s: %s" % (str(results[5]), str(ex)))
        rtime = time.time()
    return (
        str(results[0]),
        int(results[1]),
        int(results[2]),
        int(results[3]),
        int(results[4]),
        rtime,
    )


def getFStat(path):
    try:
        fs = os.stat(path)
        return [
            int(fs.st_size),
            int(fs.st_atime),
            int(fs.st_mtime),
            int(fs.st_ctime),
        ]
    except Exception as ex:
        logger("Unable to stat file %s: %s" % (str(path), str(ex)))
        return [0, 0, 0, 0]


def computeHash(path):
    try:
        h = hashlib.md5()
        f = open(str(path), "rb")
        d = f.read(RSIZE)
        while len(d) > 0:
            h.update(d)
            d = f.read(RSIZE)
        f.close()
        return h.hexdigest()
    except Exception as ex:
        logger(
            "Unable to compute md5 checksum for %s: %s" % (str(path), str(ex))
        )
    return ""


def compareStats(path, atime, mtime, ctime, size):
    fs = getFStat(path)
    if fs[1] == 0:
        return False
    if fs[0] != size or fs[1] != atime or fs[2] != mtime or fs[3] != ctime:
        return False
    return True


def hashThreads(cur, paths):
    for fullpath in paths:
        fstat = getFStat(fullpath)
        # logger("File %s: fstat = %s" % (str(fullpath), str(fstat)))
        if fstat[1] == 0:
            # logger("File %s missing" % str(fullpath))
            continue
        dbhash, atime, mtime, ctime, size, whent = getDB(cur, fullpath)
        if dbhash is None:
            # logger("Creating new md5sum for %s" % str(fullpath))
            fhash = computeHash(fullpath)
            insertDB(
                cur, fullpath, fhash, fstat[1], fstat[2], fstat[3], fstat[0]
            )
            continue
        if fstat[2] == mtime and fstat[3] == ctime and fstat[0] == size:
            # logger("Matching fstat for %s, continuing." % str(fullpath))
            continue
        # logger("fstat not matching for %s, creating new md5sum." % str(fullpath))
        # logger("atime = %d, fstat[1] = %d, mtime = %d, fstat[2] = %d, ctime = %d, fstat[3] = %d, size = %d, fstat[0] = %d" %
        # (atime, fstat[1], mtime, fstat[2], ctime, fstat[3], size, fstat[0]))
        fhash = computeHash(fullpath)
        if fhash == "" or fhash == dbhash:
            continue
        insertDB(cur, fullpath, fhash, fstat[1], fstat[2], fstat[3], fstat[0])

    exit(0)


if __name__ == "__main__":
    print(("Using %d CPUs." % NUMTHREADS))
    conn, cur = openDB()
    if conn is None:
        exit(-1)
    if len(argv) > 1:
        files = getFileList(str(argv[1]))
    else:
        files = getFileList(".")
    count = 0
    paths = {}
    for i in range(0, NUMTHREADS):
        paths[i] = []
    for path in files:
        paths[count].append(path)
        count += 1
        if count == NUMTHREADS:
            count = 0
    workers = []
    for i in range(0, NUMTHREADS):
        worker = threading.Thread(target=hashThreads, args=(cur, paths[i]))
        worker.daemon = True
        worker.start()
        workers.append(worker)
    while threading.activeCount() > 1:
        time.sleep(1.0)
    closeDB(conn, cur)
    for worker in workers:
        worker.join(0.1)
        workers.remove(worker)
    while not tqueue.empty():
        path = tqueue.get()
        print(("FIle %s updated" % str(path)))
