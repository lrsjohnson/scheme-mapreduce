(load "util")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; The main mapreduce algoritm.

;;; Sets up a master that organizes workers, dispatches tasks and
;;; coordinates communication of results.

(define *num-mappers* 5)
(define *num-reducers* 10)

(define *mappers* #f)
(define *reducers* #f)
(define *output* '())
(define yield #f)

;;; Direct implementation of mapreduce; no threads, etc.
(define (mapreduce map-func reduce-func input-data)
  (clear-output)
  (make-mappers map-func)
  (pp 'mappers-created)
  (make-reducers reduce-func)
  (pp 'reducers-created)  
  (for-each (lambda (input-data-element)
	      (send-data-to-mapper
	       input-data-element
	       (get-mapper
		(naive-partition input-data-element
				 *num-mappers*))))
	    input-data)
  (run-mappers)
  (run-reducers)
  (get-output))

;;; Naive output management

(define (clear-output)
  (set! *output* '()))

(define (add-to-output output-elt)
  (set! *output*
	(cons output-elt *output*)))

(define (get-output)
  *output*)

;;; Mappers

(define (make-mapper i map-func)
  (let ((data-for-mapper '()))
    (lambda (msg #!optional arg)
      (case msg
	((add-data)
	 (set! data-for-mapper (cons arg data-for-mapper)))
	((run)
	 (fluid-let
	     ((yield
	       (lambda (intermediate-key
			intermediate-value)
		 (mapper-to-reducer intermediate-key
				    intermediate-value))))
	   (for-each (lambda (data-elt)
		       (map-func data-elt))
		     data-for-mapper)))))))

;;;; Intermediate data structure
(define (make-intermediate-data key val)
  (list 'intermediate-data key val))

(define (intermediate-data? o)
  (and (list? o) (eq? (car o) 'intermediate-data)))

(define (intermediate-data-key intermediate-data)
  (cadr intermediate-data))
		       
(define (intermediate-data-value intermediate-data)
  (caddr intermediate-data))


(define (mapper-to-reducer intermediate-key intermediate-value)
  (send-data-to-reducer
   (make-intermediate-data intermediate-key
			   intermediate-value)
   (get-reducer (naive-partition intermediate-key *num-reducers*))))

(define (make-mappers map-func)
  (set!
   *mappers*
   (map
    (lambda (i)
      (make-mapper i map-func))
    (make-range *num-mappers*))))

(define (get-mapper mapper-id)
  (list-ref *mappers* mapper-id))

(define (send-data-to-mapper data-element mapper)
  (mapper 'add-data data-element))

(define (run-mappers)
  (for-each (lambda (mapper)
	      (run-mapper mapper))
	    *mappers*))

(define (run-mapper mapper)
  (mapper 'run))

;;; Reducers:

(define (append-data-in-hashtable hash-table
				  key
				  value)
  (hash-table/put! hash-table
		   key
		   (cons value
			 (hash-table/get hash-table
					 key
					 '()))))
(define (make-reducer i reducer-func)
  (let ((data-from-mapper (make-eq-hash-table)))
    (lambda (msg #!optional arg)
      (case msg
	((add-data)
	 (let ((intermediate-key (intermediate-data-key arg))
	       (intermediate-value (intermediate-data-value arg)))
	   (append-data-in-hashtable data-from-mapper
				     intermediate-key
				     intermediate-value)))
	((run) (hash-table/for-each
		data-from-mapper
		(make-reducer-hashtable-foreach-procedure
		 reducer-func)))))))

(define (make-reducer-hashtable-foreach-procedure reduce-func)
  (lambda (intermediate-key intermediate-values)
    (fluid-let
	((yield
	  (lambda (final-key
		   final-value)
	    (export-final-data-result
	     final-key
	     final-value))))
      (reduce-func intermediate-key
		   intermediate-values))))

(define (run-reducer reducer)
  (reducer 'run))

(define (run-reducers)
  (for-each (lambda (reducer)
	      (run-reducer reducer))
	    *reducers*))

(define (send-data-to-reducer intermediate-data
			      reducer)
  (reducer 'add-data
	   intermediate-data))

;;; Make the actual reducer objects

(define (make-reducers reducer-func)
  (set!
   *reducers*
   (map
    (lambda (i)
      (make-reducer i reducer-func))
    (make-range *num-reducers*))))
  
(define (get-reducer reducer-id)
  (list-ref *reducers* reducer-id))

;;; Parition maps data elements to mappers
  
(define (naive-partition input-data-element num-workers)
  (remainder (hash input-data-element) num-workers))



(define (export-final-data-result
	 final-key
	 final-value)
  (add-to-output (list 'result:
		       final-key
		       final-value)))

#|
;;; Data elt is a list of symb
(define (test-map data-elt)
  (for-each (lambda (symb)
	      (yield symb 1))
	    data-elt))

(define (test-reduce intermediate-key intermediate-values)
  (yield intermediate-key
	 (sum intermediate-values)))

(define test-data
	 '((a b c d)
	   (a b)
	   (c a b b a g e)))

(mapreduce test-map test-reduce test-data)
;-> ((result: c 2) (result: d 1) (result: e 1) (result: g 1) (result: b 4) (result: a 4))

|#
	    