;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; MapReduce in Scheme ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; 6.945 Final Project - Spring 2014

;;; Lars Johnson (larsj) + Tej Kanwar (gurtej)

(load "load")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Objectives:
;;;
;;; The objectives for our project is fourfold:
;;;
;;; 1) To explore the application of concepts learned in 6.945 to the
;;;    target domain of distributed systems computing by developing a
;;;    framework for the MapReduce algorithm.
;;;
;;; 2) To emphasize flexibility in designing mapreduce interfaces that
;;;    can easily permit multiple backend implementations and to build
;;;    and mix/match a handful of such implementations.
;;;
;;; 3) To extend some of the continuation / evaluator / thread / actor 
;;;    success / failure paradigms covered in lectures to see how
;;;    they can support a standardized framework for computation.
;;;
;;; 4) To experiment with search and code analysis techniques to see
;;;    if it is possible to start with a user-defined function and
;;;    extract abstractions that allow it to be processed in a
;;;    distributed manner via a MapReduce famework


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Supporting Infrastructure
;;;
;;; Our system depends on two main supporting infrastructures. For
;;; each of these infrastructures, we explored a handful of backend
;;; implementations that can be mixed and matched:
;;;
;;; 1) Worker Pools: The map reduce algorithm needs the ability to
;;;       distribute work to a number of "workers" that will perform
;;;       the tasks in parallel.
;;;
;;;       The implementations we provide for worker pools:
;;;            * Simple, single-worker scheme evaluator
;;;            * Conspire threads
;;;            * Separate Processes set up by the operating system
;;;            * Actors (from Problem Set 9).
;;;
;;; 2) Inter-worker Communication: Given these workers, we need to
;;;       have an infrastructure that permits communication between
;;;       these workers and between the workers and the main program.
;;;
;;;       The implementations we provide for inter-worker communication:
;;;            * Pipes (interlocked queues)
;;;            * Files on the host operating filesystem
;;;            * TCP Server / Clients + Ports


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; MapReduce Algorithm Implementation
;;;
;;; The MapReduce algorithm itself is built on these supporting
;;; infrastructures

;;; Master

;;; Manages communications

;;; Failures?

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Interpreter Syntax + Experimental Automatic Map-reduce abstraction
;;;
;;; ...




