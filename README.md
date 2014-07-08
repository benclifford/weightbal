weightbal
=========

iterative balancing of tests based on execution time

build
=====
apt-get install haskell-platform

cabal install

export PATH=~/.cabal/bin:$PATH

run
===

Create a test list called tests.sim, one test name per line.

For example:

```
$ cat tests.sim
1
2
3
5
8
13
```

Run one iteration like this:

 weightbal "for a in %T; do echo TEST \$a; sleep \$a; done"

This example uses the name of the test as a parameter to sleep,
and over time the test scores should converge towards the
name of the test, after a large number of runs.

Scores are stores in scores.wb and output at the end of
each run. Tests can be added or removed from tests.sim
in between runs and the change will be detected.

Commandline parameters:

  -n N or --partitions=N

    sets the number of partitions to run. By default, 4.

The rest of the command line is used as a commandline
template to run in each partition, with substitutions:

  %T means the list of tests this worker should run.

  %S means the number of the worker.


