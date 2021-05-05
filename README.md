# Distributed MapReduce

## Build Procedure

Dependencies are:
* Boost C++ Libraries 1.72
* MPI implementation (OpenMPI, MPICH or Intel MPI)

To run the code:

```
$ mkdir build
$ cd build
$ cmake ..
$ make
$ cd bin
$ mpirun -n <number of processes> --oversubscribe ./<executable> “<input directory>” - r <number of reduce workers> -m <number of reduce workers>
```

The input directory consists of many files with the input. The bin folder contains executables for the three different example programs (word count, inverted index and distributed grep.
