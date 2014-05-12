scheme-mapreduce
================

6.945 Final Project - Lars Johnson and Tej Kanwar

main.scm
    The main file. Includes:
     - functions to run the computations
     - mrs:repl
     - many test cases

data-loader.scm
    Operations for users to feed data into the system.

datasets.scm
    Generic operator interface for datasets + implementations for:
    - mrq-data-set
    - sink-data-set
    - file-writer-data-set
    - output-data-set

load.scm
    Loads the system

multi-reader-queue.scm
    Fondly known as Mr. Queue, contains implementation of our mutlti-reader, multi-writer queue which is used by data sets.
    
operations.scm
    Contains the public facing operations for perfoming computations on data sets:
    - mrs:map, mrs:filter, mrs:reduce, etc.

pipes.scm
    Generic operator interface for pipes and a queue/conspiracy-based implementation

util.scm
    Helper functions for test cases, etc.

workers.scm
    Defines distributor and worker implementations


lib/
    Third-party code taken from class assignments

examples/
    Some simple examples

