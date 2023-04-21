#!/usr/bin/python3 -uO

import logging
import pickle
import signal
import threading
import time
from datetime import datetime
from json import dumps as jdumps
from os import remove
from os.path import exists
from queue import Queue
from string import ascii_letters
from sys import exit as sysexit
from typing import Any, Dict, List

import requests
from matplotlib import pyplot as plt

# THe list of hosts to pull stats from
hostList: List[str] = []
# The base file path to write data
base: str = "/var/tmp/web_stats"
webbase: str = "/var/www/html/stats/"


# Save the collected data between invocations
def handler(signum, frame):
    logging.info("Signal received. Writing data and exiting.")
    global base
    global host_stats
    for host in host_stats:
        logging.info(f"Writing stats for {host}")
        with open(f"{base}/{host}.pickle", "wb") as f:
            f.write(host_stats[host].dumps())
    sysexit(0)


# A class to make it wasy to save stats
class stats:
    def __init__(self, hostname: str) -> None:
        self.data: Dict = {}
        self.hostname: str = hostname

    def addType(self, valType: str) -> None:
        if valType not in self.data:
            self.data[valType] = {}

    def delOldVals(self, secs_to_keep: int = 28800) -> None:
        now: int = int(time.time())
        before: int = now - secs_to_keep
        for valType in self.data:
            for valTime in self.data[valType]:
                if valTime < before:
                    del self.data[valType][valTime]

    def addVal(self, valType: str, value: Any) -> None:
        self.addType(valType)
        now = int(time.time())
        self.data[valType][now] = value

    def retVals(self, valType: str) -> List:
        retList: List = []
        if valType not in self.data:
            return []
        for datum in self.data[valType]:
            retList.append([self.data[valType][datum], datum])
        return list(sorted(retList, key=lambda x: x[1]))

    def retTypes(self) -> List[str]:
        return list(self.data.keys())

    def dumps(self) -> bytes:
        return pickle.dumps(self.data)

    def loads(self, data: bytes) -> None:
        self.data = pickle.loads(data)

    def __repr__(self) -> str:
        return str(self.data)

    def __str__(self) -> str:
        return jdumps(self.data)


# A threaded module to pull stats from a specified host and put it in a queue
def pull_stats(
    host: str, outQueue: Queue, s: requests.Session, qlock: threading.Lock
) -> None:
    logging.info(f"Starting pull_stats for host {host}")
    while True:
        # Pull every 5 minutes
        while int(time.time()) % 300 > 0:
            time.sleep(0.97)
        try:
            r = s.get(f"http://{host}.local:49152/", timeout=10)
            qlock.acquire()
            outQueue.put(r.json())
            qlock.release()
            r.close()
            time.sleep(5)
        except Exception as ex:
            logging.warning(f"Unable to pull {host}: {ex}")
            time.sleep(5)
            try:
                r.close()
            except Exception as ex:
                logging.debug(f"Couldn't close the request: {ex}")


# Take the pulled data save it and process it
def process_data(data: Dict) -> None:
    hostname = data["hostname"]
    logging.debug(f"Processing data for {hostname}")
    global host_stats
    myStats: stats = host_stats[hostname]
    myStats.delOldVals()
    for name in ["cpu", "process_counts", "load_avg", "memory", "vm"]:
        myStats.addVal(name, data[name])
    generate_graphs(hostname)
    logging.debug(f"{hostname}: {myStats}")


# Walk through the data and create pretty graphs
def generate_graphs(hostname: str) -> None:
    logging.debug(f"Entering generate_graphs for {hostname}")
    global host_stats
    global webbase
    myStats: stats = host_stats[hostname]
    filepath: str = f"{webbase}{hostname}_"
    for name in myStats.retTypes():
        sstats = myStats.retVals(name)
        x: List[datetime] = []
        match name:
            case "cpu":
                usercpu: List = []
                syscpu: List = []
                idlecpu: List = []
                x = []
                for vals in sstats:
                    x.append(datetime.fromtimestamp(vals[1]))
                    usercpu.append(vals[0]["user"])
                    syscpu.append(vals[0]["sys"])
                    idlecpu.append(vals[0]["idle"])
                plt.plot(
                    x,
                    usercpu,
                    "o-",
                    label="User CPU",
                )
                plt.plot(x, syscpu, "o-", label="System CPU")
                plt.plot(x, idlecpu, "o-", label="Idle CPU")
                plt.legend()
                plt.title(f"{hostname} CPU Percentages")
                plt.xlabel("Time")
                plt.ylabel("Percent")
                plt.savefig(f"{filepath}cpu.png", format="png")
                plt.close()
            case "process_counts":
                total: List[int] = []
                running: List[int] = []
                sleeping: List[int] = []
                x = []
                for vals in sstats:
                    x.append(datetime.fromtimestamp(vals[1]))
                    total.append(vals[0]["total"])
                    running.append(vals[0]["running"])
                    sleeping.append(vals[0]["sleeping"])
                plt.plot(x, total, "o-", label="Total Processes")
                plt.plot(x, running, "o-", label="Running Processes")
                plt.plot(x, sleeping, "o-", label="Sleeping Processes")
                plt.legend()
                plt.title(f"{hostname} Processes")
                plt.xlabel("Time")
                plt.ylabel("Number Of Processes")
                plt.savefig(f"{filepath}processes.png", format="png")
                plt.close()
            case "load_avg":
                one: List[float] = []
                five: List[float] = []
                fifteen: List[float] = []
                x = []
                for vals in sstats:
                    x.append(datetime.fromtimestamp(vals[1]))
                    one.append(vals[0]["one"])
                    five.append(vals[0]["five"])
                    fifteen.append(vals[0]["fifteen"])
                plt.plot(x, one, "o-", label="One Minute Average")
                plt.plot(x, five, "o-", label="Five Minute Average")
                plt.plot(x, fifteen, "o-", label="Fifteen Minute Average")
                plt.legend()
                plt.xlabel("Time")
                plt.ylabel("Load Averages")
                plt.title(f"{hostname} Load Averages")
                plt.savefig(f"{filepath}load_avg.png", format="png")
                plt.close()
            case "memory":
                x = []
                used: List[float] = []
                for vals in sstats:
                    x.append(datetime.fromtimestamp(vals[1]))
                    val = vals[0]["used"]
                    if str(val)[-1] in ascii_letters:
                        num = float(val[:-1])
                        if val[-1].lower() == "g":
                            num *= 1024.0
                    else:
                        num = float(val)
                    used.append(num)
                plt.plot(x, used, "o-", label="Used Memory")
                plt.legend()
                plt.xlabel("Time")
                plt.ylabel("Used Memory (In MB)")
                plt.title(f"{hostname} Memory Usage")
                plt.savefig(f"{filepath}memory.png", format="png")
                plt.close()
            case "vm":
                x = []
                if "vsize" in sstats[0][0]:  # Is this a Mac?
                    vsize: List[int] = []
                    swapin: List[int] = []
                    swapout: List[int] = []
                    for vals in sstats:
                        x.append(datetime.fromtimestamp(vals[1]))
                        swapin.append(int(vals[0]["swapin"]))
                        swapout.append(int(vals[0]["swapout"]))
                        vs = int(vals[0]["vsize"][:-1])
                        scale = vals[0]["vsize"][-1].lower()
                        if scale == "t":
                            vs *= 1024 * 1024
                        elif scale == "g":
                            vs *= 1024
                        vsize.append(vs)
                    plt.plot(x, vsize, "o-", label="Total Virtual Memory")
                    plt.plot(x, swapin, "o-", label="Swapins")
                    plt.plot(x, swapout, "o-", label="Swapouts")
                else:
                    vtotal: List[float] = []
                    vfree: List[float] = []
                    vused: List[float] = []
                    avail: List[float] = []
                    for vals in sstats:
                        x.append(datetime.fromtimestamp(vals[1]))
                        vtotal.append(vals[0]["total"])
                        vfree.append(vals[0]["free"])
                        vused.append(vals[0]["used"])
                        avail.append(vals[0]["available"])
                    plt.plot(x, vtotal, "o-", label="Total Available Swap")
                    plt.plot(x, vfree, "o-", label="Free Swap")
                    plt.plot(x, vused, "o-", label="Used Swap")
                    plt.plot(x, avail, "o-", label="Total Avaiable VM")
                plt.legend()
                plt.title(f"{hostname} Virtual Memory")
                plt.xlabel("Time")
                plt.ylabel("Memory (In MB)")
                plt.savefig(f"{filepath}vm.png", format="png")
                plt.close()
            case _:
                logging.warning(f"Unknown data type {name}. Skipping.")
                continue


# Pull the data off the queue and do something with it
def process_queue(dataQueue: Queue, qlock: threading.Lock) -> None:
    logging.info("Starting process_data")
    while True:
        qlock.acquire()
        no_data: bool = dataQueue.empty()
        qlock.release()
        while no_data:
            time.sleep(10)
            qlock.acquire()
            no_data = dataQueue.empty()
            qlock.release()
        qlock.acquire()
        data: Dict = dataQueue.get()
        qlock.release()
        process_data(data)


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:\t%(message)s",
        level=logging.INFO,
    )
    # Catch the quit signals to save the data
    signal.signal(signal.SIGHUP, handler)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    global host_stats
    # Start the background threads to pull the data
    host_stats: Dict[str, stats] = {}
    workerList: List[threading.Thread] = []
    dataQueue: Queue = Queue(maxsize=0)
    qlock: threading.Lock = threading.Lock()
    s: requests.Session = requests.Session()
    for name in hostList:
        host_stats[name] = stats(name)
        fname = f"{base}/{name}.pickle"
        # Restore the saved data, if it exists.
        if exists(fname):
            logging.info(f"Loading saved data for {name}")
            with open(fname, "rb") as f:
                host_stats[name].loads(f.read())
            remove(fname)
        worker: threading.Thread = threading.Thread(
            target=pull_stats,
            args=(name, dataQueue, s, qlock),
            name=name,
            daemon=True,
        )
        worker.start()
        workerList.append(worker)
    # Start the background thread to clear the queue
    worker = threading.Thread(
        target=process_queue,
        args=(dataQueue, qlock),
        name="process_queue",
        daemon=True,
    )
    worker.start()
    workerList.append(worker)
    # Remove any dead workers
    while True:
        time.sleep(600)
        for worker in workerList:
            if not worker.is_alive():
                worker.join(0.1)
                logging.info(f"Removed thread {worker.name}")
                workerList.remove(worker)
