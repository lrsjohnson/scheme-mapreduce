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

;;; Failures / Scheduling
;;;; - maybe different ways of handling failures / scheduling to
;;;; increase the performance.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Interpreter Syntax + Experimental Automatic Map-reduce abstraction
;;;
;;; User-facing interface...
;;;
;;; Possibly "analyzing"

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; MapReduce Computation Model:
;;;
;;; map: Input is a single (key-type-1  value-type-1)
;;;      -> List of pairs (key-type-2 value-type-2)
;;;
;;; reduce: Input is (key-type-2 (list of value-type-2))
;;;         -> Output is a list of value-type-2's

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Some sample applications of MapReduce:
;;;
;;; [Based on discussion in MapReduce paper by Dean + Ghemawat 2004]
;;;
;;; Word Frequency:
;;;   map: (document-name document-contents) -> list of (word count)
;;;   reduce: (word (list of count)) to sum of counts
;;;
;;; Distributed Grep:
;;;   map: (document-name doc-contents) -> list of ('unused lines)
;;;   reduce: identity

;;; Count of URL Access Frequency:
;;;   map: ('unused 




