# Multiproc

A simple CLI tool for quickly executing many similar commands in parallel.

## Example

Pretend there is a script `script.py` in your working directory that takes an integer command line argument. Then multiproc allows quickly executing this script with arguments specified in a range:

```
multiproc.py "python script.py %0" -m 0:8:2
```

This will run the following commands in parallel on subprocesses:

```
python script.py 0
python script.py 2
python script.py 4
python script.py 6
python script.py 8
```

We can also combine command modifiers in a straightforward way:

```
multiproc.py "python script.py %0 %1" -m 2 3
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

Output is stored in an `mpout` folder created in the working directory. Logging info from each process is stored in `mpout/../log`.

The number of processes to use defaults to the number of (virtual) cores available but can be modified with `-j`.

It's often useful to use multiproc with `nohup` and `&` to have everything running in the background. In this case, it is up to you to clean up.
