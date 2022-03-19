# Multiproc 

A simple CLI tool for quickly executing many similar commands in parallel.

## Example

Pretend there is a script `script.py` in your working directory that takes an integer command line argument. Multiproc allows quickly executing this script with arguments specified in a range, referred to as a modifier. 

```
multiproc.py run "python script.py %0" -m 0:8:2
```

This will run the following commands in parallel on subprocesses:

```
python script.py 0
python script.py 2
python script.py 4
python script.py 6
python script.py 8
```

(A note about modifiers: A modifier range is not the same as a python range. Notably, it is not zero indexed. For example, `3` expands to `1, 2, 3`, `4:6` expands to `4, 5, 6` and `3:10:2` exapnds to `3, 5, 7, 9` ("three to ten in steps of two").

We can also combine command modifiers in a straightforward way:

```
multiproc.py run "python script.py %0 %1" -m 2 3
```

which results in:

```
python script.py 1 1
python script.py 1 2
python script.py 1 3
python script.py 2 1
python script.py 2 2
python script.py 2 3
```

This can be scaled to as many modifiers as you want but this is as complex as command modifiers get. Any additional complexity should be handled in your script. 
Output is stored in an `mpout` folder created in the working directory with logging info from each process stored in `mpout/../log`.

The number of processes to use defaults to the number of (virtual) cores available but can be modified with `-j`.

It's often useful to use multiproc with `nohup` and `&` to have everything running in the background.To cancel jobs started in the background use the `kill` command: `multiproc.py kill`. This kills all jobs started in the working directory.
