#!/usr/bin/env python

from __future__ import print_function

import os, sys, glob, time, datetime, shlex
import argparse
import math
import multiprocessing
import subprocess

from multiprocessing.pool import ThreadPool
from multiprocessing.dummy import Pool
from itertools import product, repeat
from functools import partial
from hashlib import md5

def worker(cmd, out_file, log, shell):
    """ Spawn the next process. """

    with open(out_file, 'wb') as fp:

        t = time.time()
        if shell:
            p = subprocess.Popen(
                    cmd,
                    stdout=fp,
                    stderr=fp,
                    shell=shell
                )
            log.put(f"<pid:{p.pid}> Executing command: \"{cmd}\"")
            p.wait()
        else:
            cmds = cmd.split("|")
            p1 = subprocess.Popen(
                    shlex.split(cmds[0]),
                    stdout=subprocess.PIPE,
                    stderr=fp
                )
            log.put(f"<pid:{p1.pid}> Executing command: \"{cmds[0]}\"")
            for c in cmds[1:]:
                p2 = subprocess.Popen(
                        shlex.split(c),
                        stdin=p1.stdout,
                        stdout=subprocess.PIPE,
                        stderr=fp
                    )
                p1.stdout.close()
                log.put(f"<pid:{p2.pid}> Piping command to <pid:p1.pid>: \"{c}\"")
                p1 = p2

            outs, errs = p1.communicate()
            fp.write(outs)
            p = p1

    return p, cmd, time.time() - t, log

def callback(result):
    """ Callback triggered when the threadpool completes a job."""
    p, cmd, t, log = result
    log.put(f"<pid:{p.pid}> Execution time: {t} seconds.")
    if p.returncode == 0:
        log.put(f"<pid:{p.pid}> Completed successfully.")
    else:
        log.put(f"<pid:{p.pid}> Failed with exit status {p.returncode}.")

def gen_cmds(cmd, modifiers):
    """ Create a generator for modified commands."""
    
    # only support modifiers %0-%9
    if modifiers is None:
        yield cmd, "mp.out"
    else:
        assert len(modifiers) < 11
        
        products = product(*modifiers)
        x = ["%"+str(i) for i in range(len(modifiers))]
        for prod in products:
            suffix = "".join(["_"+str(i) for i in list(prod)])

            new_cmd = cmd
            s = [str(p) for p in prod]
            for sub in zip(x, s):
                new_cmd = new_cmd.replace(*sub)

            yield new_cmd, "mp"+suffix+".out"

def logger(log, log_file):
    """ listen for log messages and write to the log file."""
    t = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    with open(log_file, "a") as fp:
        while True:
            m = log.get()
            if m == 'kill':
                fp.write(f"{t}: Done.\n")
                break
            fp.write(f"{t}: {m}\n")
            fp.flush()
    
def execute(args):
    """ Distribute commands over multiple processes."""

    # truncate hash for readability
    cmd_hash = md5(bytes(args.command, "ascii")).hexdigest()[0:8]
    if args.modifiers is None:
        num_jobs = 1
    else:
        num_jobs = math.prod([len(a) for a in args.modifiers])

    # create mpout directories if they don't exist
    mpout_dir = "mpout"
    if not os.path.exists(mpout_dir):
        os.mkdir(mpout_dir)

    t = str(int(time.time()))
    out_dir = os.path.join(mpout_dir, cmd_hash+"-"+t)
    if os.path.exists(out_dir):
        error("Hash or time collision? Try again.")
    else:
        os.mkdir(out_dir)

    # manager to queue log messages
    manager = multiprocessing.Manager()
    log = manager.Queue()    
    
    # start threadpool to manage processes
    if args.num_proc == 0:
        args.num_proc = multiprocessing.cpu_count()
    num_proc = min(args.num_proc, num_jobs)
    pool = ThreadPool(processes=num_proc+1)

    #put logging thread to work first
    log_file = os.path.join(out_dir, "log")
    log_listener = pool.apply_async(logger, (log, log_file))

    log.put("Input: {}".format(" ".join(sys.argv)))
    log.put(f"Output: {out_dir}")
    log.put(f"Total of {num_jobs} jobs.")
    log.put(f"Initialized worker pool of size {num_proc}.")

    # spawn workers
    jobs = []
    for cmd, fn in gen_cmds(args.command, args.modifiers):
        out = os.path.join(out_dir, fn)
        job = pool.apply_async(worker, (cmd, out, log, args.shell), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    # kill listener
    log.put('kill')

    # close and join threadpool
    pool.close()
    pool.join()

def modifier_to_range(s):
    mod = [int(i) for i in s.split(':')]
    n = len(mod)

    if n == 1:
        return range(*[1, mod[0]+1])
    elif n == 2 or n == 3:
        mod[1] += 1
        return range(*mod)
    elif n > 3:
        error("Invalid modifiers.")
    
    return range(0)

if __name__ == '__main__':
    desc = "A simple CLI tool for quickly executing many similar commands in parallel."
    parser = argparse.ArgumentParser(description=desc, prog="multiproc")
    parser.add_argument("command", help="Input command.")
    parser.add_argument("-m", "--modifiers", help="Command modifiers.", nargs="+", 
            type=modifier_to_range)
    parser.add_argument("-j", help="""Specify the number of threads to spawn. Uses the number 
        of (virtual) cores by default, plus one for logging.""", action="store", type=int, 
        dest="num_proc", default=0)
    parser.add_argument("-s", "--shell", help="""Use shell pipeline support. Only for trusted 
        input. This can help if pipes/redirects are not working properly.""", action="store_true",
        dest="shell", default=False)
    
    args = parser.parse_args()
    execute(args)
