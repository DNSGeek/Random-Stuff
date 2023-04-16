#!/usr/bin/python3 -uO

import logging
import subprocess
import sys
from platform import node
from typing import Dict, List


def get_top() -> List[str]:
    try:
        top = subprocess.Popen(
            ["/usr/bin/top", "-b", "-n", "1"],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        lines = top.stdout.readlines()
        retList: List[str] = []
        for line in lines:
            retList.append(line.decode("utf-8").strip())
        return retList
    except Exception as ex:
        logging.error(f"Unable to process top: {ex}")
        sys.exit(1)


def parse_top(top: List[str]) -> Dict:
    retDict: Dict = {"hostname": node()}
    for line in top:
        if not line:
            continue  # skip blank lines
        fields = line.split()
        if line.startswith("top - "):
            one = float(fields[12][:-1])
            five = float(fields[13][:-1])
            fifteen = float(fields[14])
            retDict["load_avg"] = {
                "one": one,
                "five": five,
                "fifteen": fifteen,
            }
        elif line.startswith("Tasks:"):
            total = int(fields[1])
            running = int(fields[3])
            sleeping = int(fields[5])
            stopped = int(fields[7])
            zombie = int(fields[9])
            retDict["process_counts"] = {
                "total": total,
                "running": running,
                "sleeping": sleeping,
                "stopped": stopped,
                "zombie": zombie,
            }
        elif line.startswith("%Cpu(s):"):
            userpct = float(fields[1])
            syspct = float(fields[3])
            nicepct = float(fields[5])
            idlepct = float(fields[7])
            waitpct = float(fields[9])
            hardpct = float(fields[11])
            softpct = float(fields[13])
            stolenpct = float(fields[15])
            retDict["cpu"] = {
                "user": userpct,
                "sys": syspct,
                "idle": idlepct,
                "nice": nicepct,
                "wait": waitpct,
                "hardware_int": hardpct,
                "software_int": softpct,
                "hypervisor": stolenpct,
            }
        elif line.startswith("MiB Mem"):
            total = float(fields[3])
            freemem = float(fields[5])
            used = float(fields[7])
            cache = float(fields[9])
            retDict["memory"] = {
                "used": used,
                "free": freemem,
                "total": total,
                "cache": cache,
            }
        elif line.startswith("MiB Swap:"):
            total = float(fields[2])
            freeswp = float(fields[4])
            used = float(fields[6])
            avail = float(fields[8])
            retDict["vm"] = {
                "total": total,
                "free": freeswp,
                "used": used,
                "available": avail,
            }
        elif fields[0] == "PID":
            out_fields: Dict[str, int] = {}
            for lnum, name in enumerate(fields):
                out_fields[name] = lnum
        else:
            if "processes" not in retDict:
                retDict["processes"] = []
            pdict = {}
            for name in out_fields:
                pdict[name.lower()] = fields[out_fields[name]]
            retDict["processes"].append(pdict)
    return retDict


if __name__ == "__main__":
    top = get_top()
    data = parse_top(top)
    print(data)
