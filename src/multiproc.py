#!/usr/bin/env python

from __future__ import print_function

import os, signal, sys, glob, time, datetime, shlex
import argparse
import math
import multiprocessing as mp
import subprocess
#import threading
from multiprocessing.pool import ThreadPool
from multiprocessing.dummy import Pool
from itertools import product, repeat
from functools import partial
from hashlib import md5

mpout_dir = "mpout"
pid_file = os.path.join(mpout_dir, "pids")

def worker(cmd, out_file, log, kill, shell):
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
            log.put(f"<pid:{p.pid}> Executing command (shell=True): \"{cmd}\"")
            p.wait()
        else:
            cmds = cmd.split("|")
            if len(cmds) == 1:
                p = subprocess.Popen(
                        shlex.split(cmds[0]),
                        stdout=fp,
                        stderr=fp
                    )
            elif len(cmds) == 2:
                p1 = subprocess.Popen(
                        shlex.split(cmds[0]),
                        stdout=subprocess.PIPE,
                        stderr=fp
                    )
                p2 = subprocess.Popen(
                        shlex.split(cmds[1]),
                        stdin=p1.stdout,
                        stdout=fp,
                        stderr=fp
                    )
                log.put(f"<pid:{p2.pid}> Executing command: \"{cmd}\"")
                p1.stdout.close()
                p = p2
            else:
                p1 = subprocess.Popen(
                        shlex.split(cmds[0]),
                        stdout=subprocess.PIPE,
                        stderr=fp
                    )
                for c in cmds[1:-1]:
                    p2 = subprocess.Popen(
                            shlex.split(c),
                            stdin=p1.stdout,
                            stdout=subprocess.PIPE,
                            stderr=fp
                        )
                    p1.stdout.close()
                    p1 = p2
                p = subprocess.Popen(
                        shlex.split(cmds[-1]),
                        stdin=p1.stdout,
                        stdout=fp,
                        stderr=fp
                    )
                log.put(f"<pid:{p.pid}> Executing command: \"{cmd}\"")
                p1.stdout.close()

    manually_killed = False
    while True:
        # job finished
        if not p.poll() is None:
            break

        # received manual kill
        try:
            m = kill.get(timeout=0.5)

            # propagate kill command to other processes
            kill.put('kill')

            manually_killed = True
            log.put(f"<pid:{p.pid}> Cleaning up.")
            p.kill()
            break
        except:
            pass

    return p, cmd, time.time() - t, log, manually_killed

def callback(result):
    """ Callback triggered when the threadpool completes a job."""
    p, cmd, t, log, manually_killed = result
    log.put(f"<pid:{p.pid}> Execution time: {t} seconds.")
    if manually_killed:
        log.put(f"<pid:{p.pid}> Manually killed.")
    elif p.returncode == 0:
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
    
def add_pid(pid):
    with open(pid_file, "a") as fp:
        fp.write(f"{pid}\n")

def remove_pid(pid):
    pids = []
    with open(pid_file, "r") as fp:
        lines = fp.readlines()
        pids = [int(pid) for pid in lines]

    pids.remove(pid)
    with open(pid_file, "w") as fp:
        for p in pids:
            fp.write(f"{p}\n")

def run(args):
    """ Distribute commands over multiple processes."""

    # truncate hash for readability
    cmd_hash = md5(bytes(args.command, "ascii")).hexdigest()[0:8]
    if args.modifiers is None:
        num_jobs = 1
    else:
        num_jobs = math.prod([len(a) for a in args.modifiers])

    # create mpout directories if they don't exist
    if not os.path.exists(mpout_dir):
        os.mkdir(mpout_dir)

    add_pid(os.getpid())

    i = 0
    out_dir = os.path.join(mpout_dir, cmd_hash)
    while True:
        if os.path.exists(out_dir):
            i += 1
            out_dir = os.path.join(mpout_dir, cmd_hash + f"_{i}")
        else:
            break
    os.mkdir(out_dir)
    
    # manager to queue log messages
    manager = mp.Manager()
    log = manager.Queue()
    kill = manager.Queue()
    
    # start threadpool to manage processes
    if args.num_proc == 0:
        args.num_proc = mp.cpu_count()
    num_proc = min(args.num_proc, num_jobs)
    pool = ThreadPool(processes=num_proc)

    #put logging process to work first
    log_file = os.path.join(out_dir, "log")
    log_listener = mp.Process(target=logger, args=(log, log_file))
    log_listener.start()

    log.put("Input: {}".format(" ".join(sys.argv)))
    log.put(f"Output: {out_dir}")
    log.put(f"Threadpool pid: {os.getpid()}")
    log.put(f"Total of {num_jobs} jobs.")
    log.put(f"Initialized worker pool of size {num_proc}.")

    # spawn workers
    jobs = []
    #recv, send = mp.Pipe(duplex=False)
    for cmd, fn in gen_cmds(args.command, args.modifiers):
        out = os.path.join(out_dir, fn)
        job = pool.apply_async(worker, (cmd, out, log, kill, args.shell), callback=callback)
        jobs.append(job)

    # handle signals to clean up child processes properly
    def signal_handler(sig, frame):
        log.put(f'Received signal {sig}: {signal.strsignal(sig)}. Cleaning up.')

        # wait for threads to clean up
        kill.put('kill')
        pool.close()
        pool.join()

        # kill log listener last
        log.put('kill')

        # remove pid from list of running pids
        remove_pid(os.getpid())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)

    for job in jobs:
        job.get()
    
    # kill listener
    log.put('kill')
    log_listener.join()

    # close and join threadpool
    pool.close()
    pool.join()
    remove_pid(os.getpid())

def kill(args):
    with open(os.path.join(mpout_dir, "pids"), "r") as fp:
        lines = fp.readlines()
        for pid in lines:
            os.kill(int(pid), signal.SIGTERM)


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
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    run_parser = subparsers.add_parser("run", help="Run a new job.")
    run_parser.add_argument("command", help="Input command.")
    run_parser.add_argument("-m", "--modifiers", help="Command modifiers.", nargs="+", 
            type=modifier_to_range)
    run_parser.add_argument("-j", help="""Specify the number of threads to spawn. Uses the number 
        of (virtual) cores by default, plus one for logging.""", action="store", type=int, 
        dest="num_proc", default=0)
    run_parser.add_argument("-s", "--shell", help="""Use shell pipeline support. Only for trusted 
        input. This can help if pipes/redirects are not working properly.""", action="store_true",
        dest="shell", default=False)

    kill_parser = subparsers.add_parser("kill", help="Kill jobs in progress.")
    
    args = parser.parse_args()

    if args.subcommand == "run":
        #mp.set_start_method('spawn')
        run(args)
    elif args.subcommand == "kill":
        kill(args)
    else:
        error("Not a valid subcommand.")

