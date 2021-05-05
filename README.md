# Distributed MapReduce

MapReduce programming model is a versatile and elegant programming model. A lot of real world computations can be easily mapped to it. The MapReduce library simplifies writing distributed programs by abstracting away the details of the underlying distributed nature of the computations. It presents a remarkably simplified user interface where the user presents the business logic in the form of two functions: map and reduce. We demonstrated that users can quickly gain significant performance improvements by parallelizing their programs using our efficient clean fault-tolerant implementation of MapReduce library with minimal effort.

Ref: [MapReduce Paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

This is our own implementation of MapReduce Library, written in C++ for our Distributed Systems Monsoon 2021 (IIIT Hyderabad) course project.

**Originally by:**
- Yashas Samaga -- 20171080
- Sanjana Sunil -- 20171027
- Aditya Morolia -- 20171177

We also implement handling worker failures and load balancing in the library. We implement `wordCount`, `grepper` and `invertedIndex` as examples.

Our MapReduce library is a header-only library. The header files are present in the `mapreduce/include` directory.

## Build Procedure

Dependencies are:
* Boost C++ Libraries 1.72
* Boost MPI

## Usage

```bash
- mkdir build
- cd build
- cmake ..
- make
- cd bin
- mpirun -n <number of processes> --oversubscribe ./<executable> “<input directory>” - r <number of reduce workers> -m <number of reduce workers>
```

## Directory Structure of the library
```bash
├── mapreduce
│   ├── CMakeLists.txt
│   └── include
│       └── mapreduce
│           ├── combiner.hpp
│           ├── datasource
│           │   ├── datasource.hpp
│           │   └── directory_source.hpp
│           ├── job.hpp
│           ├── map.hpp
│           ├── mapreduce.hpp
│           ├── reduce.hpp
│           ├── specification.hpp
│           └── storage
│               ├── in_memory.hpp
│               └── storage.hpp
├── README.md
```

The input directory consists of many text files with the input. After the build, the bin folder contains executables for the three different example programs (word count, inverted index and distributed grep. You can run them of the text files to benchmark.
