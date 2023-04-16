#!/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -uO

import logging
import subprocess
import sys
import time
from platform import node
from typing import Dict, List


def get_top() -> List[str]:
    try:
        top = subprocess.Popen(
            ["/usr/bin/top", "-l", "1"],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        lines = top.stdout.readlines()
        top.terminate()
        retList: List[str] = []
        for line in lines:
            retList.append(line.decode("utf-8").strip())
        return retList
    except Exception as ex:
        logging.error(f"Unable to process top: {ex}")
        sys.exit(1)


def parse_top(top: List[str]) -> Dict:
    year = time.localtime().tm_year
    retDict: Dict = {"hostname": node()}
    for line in top:
        if not line:
            continue  # skip blank lines
        if line.startswith(f"{year}/"):
            continue
        fields = line.split()
        if line.startswith("Processes"):
            total = int(fields[1])
            running = int(fields[3])
            sleeping = int(fields[5])
            threads = int(fields[7])
            retDict["process_counts"] = {
                "total": total,
                "running": running,
                "sleeping": sleeping,
                "threads": threads,
            }
        elif line.startswith("Load Avg"):
            one = float(fields[2][:-1])
            five = float(fields[3][:-1])
            fifteen = float(fields[4])
            retDict["load_avg"] = {
                "one": one,
                "five": five,
                "fifteen": fifteen,
            }
        elif line.startswith("CPU usage"):
            userpct = float(fields[2][:-1])
            syspct = float(fields[4][:-1])
            idlepct = float(fields[6][:-1])
            retDict["cpu"] = {
                "user": userpct,
                "sys": syspct,
                "idle": idlepct,
            }
        elif line.startswith("SharedLibs"):
            resident = fields[1]
            data = fields[3]
            linked = fields[5]
            retDict["libs"] = {
                "resident": resident,
                "data": data,
                "linked": linked,
            }
        elif line.startswith("MemRegions"):
            total = int(fields[1])
            resident = fields[3]
            private = fields[5]
            shared = fields[7]
            retDict["regions"] = {
                "total": total,
                "resident": resident,
                "private": private,
                "shared": shared,
            }
        elif line.startswith("PhysMem"):
            used = fields[1]
            wired = fields[3]
            comp = fields[5]
            unused = fields[7]
            retDict["memory"] = {
                "used": used,
                "wired": wired,
                "compressed": comp,
                "unused": unused,
            }
        elif line.startswith("VM"):
            vsize = fields[1]
            frame = fields[3]
            swapin = fields[6].split("(")[0]
            swapout = fields[8].split("(")[0]
            retDict["vm"] = {
                "vsize": vsize,
                "framework_vsize": frame,
                "swapin": swapin,
                "swapout": swapout,
            }
        elif line.startswith("Networks"):
            pin = fields[2].split("/")
            pout = fields[4].split("/")
            pktin = int(pin[0])
            memin = pin[1]
            pktout = int(pout[0])
            memout = pout[1]
            retDict["network"] = {
                "packets_in": pktin,
                "data_in": memin,
                "packets_out": pktout,
                "data_out": memout,
            }
        elif line.startswith("Disks"):
            r = fields[1].split("/")
            w = fields[3].split("/")
            pktsin = int(r[0])
            memin = r[1]
            pktsout = int(w[0])
            memout = w[1]
            retDict["disk_usage"] = {
                "sectors_read": pktsin,
                "data_read": memin,
                "sectors_written": pktsout,
                "data_written": memout,
            }
        elif line.startswith("PID"):
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
