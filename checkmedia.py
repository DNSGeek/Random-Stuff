#!/usr/bin/python -uROO

import fnmatch
import hashlib
import multiprocessing
import os
import queue
import sqlite3
import threading
import time
from sys import argv, exit
from typing import Optional

tlock = threading.Lock()
tqueue: queue.Queue[str] = queue.Queue()
NUMTHREADS: int = max(
    2, multiprocessing.cpu_count() // 2
)
RSIZE: int = 1024 * 1024

# Type alias for the 4-element fstat list: [size, atime, mtime, ctime]
FStat = list[int]

# Type alias for the 6-element DB row: (hash, atime, mtime, ctime, size, whent)
# whent is time.struct_time on success or float (time.time()) on parse failure
DBRow = tuple[
    Optional[str],
    Optional[int],
    Optional[int],
    Optional[int],
    Optional[int],
    Optional[time.struct_time | float],
]

# Type alias for a pending batch insert row: (path, hash, atime, mtime, ctime, size)
InsertRow = tuple[str, str, int, int, int, int]


def logger(msg: str) -> None:
    with tlock:
        print(msg)


def getFileList(rundir: str) -> list[str]:
    try:
        filelist: list[str] = []
        for root, dirs, files in os.walk(rundir):
            for item in fnmatch.filter(files, "*"):
                full: str = os.path.join(root, item.strip())
                filelist.append(full)
    except Exception as ex:
        logger("Unable to get directory listing: %s" % str(ex))
        exit(-1)

    return sorted(filelist)


def openDB(
    dbfile: str = "/var/tmp/media.db",
) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    try:
        conn: sqlite3.Connection = sqlite3.connect(
            dbfile, check_same_thread=False
        )
        cur: sqlite3.Cursor = conn.cursor()
        conn.text_factory = str
        cur.executescript(
            "PRAGMA auto_vacuum = 2;"
            "PRAGMA encoding = 'UTF-8';"
            "PRAGMA temp_store = MEMORY;"
            "PRAGMA journal_mode = WAL;"
            "PRAGMA synchronous = NORMAL;"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS media ("
            "path TEXT NOT NULL, "
            "hash TEXT NOT NULL, "
            "atime UNSIGNED INT NOT NULL, "
            "mtime UNSIGNED INT NOT NULL, "
            "ctime UNSIGNED INT NOT NULL, "
            "size UNSIGNED INT NOT NULL, "
            "whent DATETIME DEFAULT current_timestamp"
            ");"
        )
        conn.commit()
    except Exception as ex:
        logger("Unable to open media DB: %s" % str(ex))
        exit(-2)
    return (conn, cur)


def closeDB(conn: sqlite3.Connection, cur: sqlite3.Cursor) -> None:
    try:
        conn.commit()
        cur.execute("CREATE INDEX IF NOT EXISTS pathidx ON media(path);")
        conn.commit()
        cur.execute("VACUUM;")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as ex:
        logger("Unable to close DB: %s" % str(ex))
        exit(-3)


def flushDB(
    cur: sqlite3.Cursor,
    batch: list[InsertRow],
    updated_paths: list[str],
) -> None:
    """Write a batch of pending inserts to the DB in a single locked transaction.
    Each element of batch is (path, hash, atime, mtime, ctime, size).
    updated_paths receives the paths that were successfully written."""
    if not batch:
        return
    with tlock:
        try:
            cur.executemany(
                "INSERT INTO media VALUES(?, ?, ?, ?, ?, ?, current_timestamp);",
                batch,
            )
            for row in batch:
                tqueue.put(row[0])
        except Exception as ex:
            logger("Unable to batch-insert into media DB: %s" % str(ex))


def getDB(cur: sqlite3.Cursor, path: str) -> DBRow:
    with tlock:
        try:
            cur.execute(
                "SELECT hash, atime, mtime, ctime, size, whent FROM media WHERE path=? ORDER BY whent DESC LIMIT 1;",
                (path,),
            )
            results: Optional[tuple] = cur.fetchone()
        except Exception as ex:
            logger("Unable to get data from media DB: %s" % str(ex))
            return (None, None, None, None, None, None)

    if results is None or len(results) < 6:
        return (None, None, None, None, None, None)

    rtime: time.struct_time | float
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


def getFStat(path: str) -> FStat:
    try:
        fs: os.stat_result = os.stat(path)
        return [
            int(fs.st_size),
            int(fs.st_atime),
            int(fs.st_mtime),
            int(fs.st_ctime),
        ]
    except Exception as ex:
        logger("Unable to stat file %s: %s" % (path, str(ex)))
        return [0, 0, 0, 0]


def computeHash(path: str) -> str:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(RSIZE):
                h.update(chunk)
        return h.hexdigest()
    except Exception as ex:
        logger("Unable to compute hash for %s: %s" % (path, str(ex)))
    return ""


def compareStats(
    path: str, atime: int, mtime: int, ctime: int, size: int
) -> bool:
    fs: FStat = getFStat(path)
    if fs[1] == 0:
        return False
    return (
        fs[0] == size and fs[1] == atime and fs[2] == mtime and fs[3] == ctime
    )


def hashThreads(cur: sqlite3.Cursor, paths: list[str]) -> None:
    batch: list[InsertRow] = []

    for fullpath in paths:
        fstat: FStat = getFStat(fullpath)
        if fstat[1] == 0:
            continue

        dbhash, atime, mtime, ctime, size, whent = getDB(cur, fullpath)

        if dbhash is None:
            fhash: str = computeHash(fullpath)
            if fhash:
                batch.append(
                    (fullpath, fhash, fstat[1], fstat[2], fstat[3], fstat[0])
                )
            continue

        # Skip if mtime, ctime, and size all match — file hasn't changed
        if fstat[2] == mtime and fstat[3] == ctime and fstat[0] == size:
            continue

        fhash = computeHash(fullpath)
        if fhash and fhash != dbhash:
            batch.append(
                (fullpath, fhash, fstat[1], fstat[2], fstat[3], fstat[0])
            )

    flushDB(cur, batch, [])


if __name__ == "__main__":
    print("Using %d CPUs." % NUMTHREADS)
    conn, cur = openDB()
    if conn is None:
        exit(-1)

    files: list[str] = getFileList(str(argv[1]) if len(argv) > 1 else ".")

    # Distribute files round-robin across worker buckets
    paths: list[list[str]] = [[] for _ in range(NUMTHREADS)]
    for i, path in enumerate(files):
        paths[i % NUMTHREADS].append(path)

    workers: list[threading.Thread] = []
    for i in range(NUMTHREADS):
        worker = threading.Thread(target=hashThreads, args=(cur, paths[i]))
        worker.daemon = True
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()

    closeDB(conn, cur)

    while True:
        try:
            path = tqueue.get_nowait()
            print("File %s updated" % path)
        except queue.Empty:
            break
