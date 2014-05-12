(define *output-callback* #f)

(define (mrs:run-computation-with-callback thunk callback)
  (with-time-sharing-conspiracy
   (lambda ()
     (let ((output-done #f))
       (fluid-let
           ((*output-callback*
             (lambda (ds-elt)
               (cond ((ds-elt-done? ds-elt)
                      (set! output-done #t))
                     (else
                      (callback 
                       (ds-elt-key ds-elt)
                       (ds-elt-value ds-elt)))))))
	 (clear-data-sets)
         (thunk)
         (flush-input-data-sets)
         (let lp ()
           (conspire:thread-yield)
           (if output-done
               'done
               (lp))))))))

(define (mrs:run-computation thunk)
  (let ((output-value '()))
    (let ((output-callback
           (lambda (k v)
             (set! output-value
                   (cons (list k v)
                         output-value)))))
      (mrs:run-computation-with-callback thunk output-callback)
      output-value)))

#|
  (define (test-print-streaming)
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (mrs:print-streaming ds-input 'ds1)
    (mrs:feed-value-list ds-input '(1 2 3 4)))
  (mrs:run-computation test-print-streaming)
 ;->  (ds1 0 1)
 ;    (ds1 1 2)
 ;    (ds1 2 3)
 ;    (ds1 3 4)
 ;    (ds1 done)

  (define (test-basic-map))
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 10 value)))
     ds-input
     ds1)
    (mrs:print-streaming ds1 'ds1)
    (mrs:feed-value-list ds-input '(1 2 3 4)))
  (mrs:run-computation test-basic-map)
 ;   (ds1 3 40)
 ;   (ds1 0 10)
 ;   (ds1 1 20)
 ;   (ds1 2 30)
 ;   (ds1 done)

 (define (test-chain-of-maps)
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 10 value)))
     ds-input
     ds1)
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 11 value)))
     ds-input
     ds1)
    (mrs:print-streaming ds1 'ds1)
    (mrs:feed-value-list ds-input '(1 2 3 4)))
  (mrs:run-computation test-chain-of-maps)
 ;  (ds1 1 20)
 ;  (ds1 2 30)
 ;  (ds1 3 40)
 ;  (ds1 0 10)
 ;  (ds1 1 22)
 ;  (ds1 2 33)
 ;  (ds1 3 44)
 ;  (ds1 0 11)
 ;  (ds1 done)

  (define (test-aggregate)
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (mrs:aggregate
     ds-input
     ds1)
    (mrs:print-streaming ds1 'ds1)
    (mrs:feed-value-list ds-input '(1 2 3 4))
    (mrs:feed-value-list ds-input '(3 3 3 3))
    (mrs:feed-value-list ds-input '(1 4 3 4)))
  (mrs:run-computation test-aggregate)
 ; (ds1 3 (4 3 4))
 ; (ds1 2 (3 3 3))
 ; (ds1 1 (4 3 2))
 ; (ds1 0 (1 3 1))
 ; (ds1 done)

 (define (test-map-reduce)
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (define ds2 (mrs:create-data-set))
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 10 value)))
     ds-input
     ds1)
    (mrs:reduce
     (lambda (key values)
       (mrs:emit key (apply + values)))
     ds1
     ds2)
    (mrs:print-streaming ds1 'ds1)
    (mrs:print-streaming ds2 'ds2)
    (mrs:feed-value-list ds-input '(1 1 3 5))
    (mrs:feed-value-list ds-input '(2 2 1 4))
    (mrs:feed-value-list ds-input '(3 1 0 3)))
  (mrs:run-computation test-map-reduce)
 ; (ds1 3 50)
 ; (ds1 0 20)
 ; (ds1 0 10)
 ; (ds1 1 10)
 ; (ds1 2 30)
 ; (ds1 0 30)
 ; (ds1 1 10)
 ; (ds1 1 20)
 ; (ds1 2 10)
 ; (ds1 3 40)
 ; (ds1 2 0)
 ; (ds1 3 30)
 ; (ds1 done)
 ; (ds2 3 120)
 ; (ds2 2 40)
 ; (ds2 1 40)
 ; (ds2 0 60)
 ; (ds2 done)


 ; (ds1 3 (4 3 4))
 ; (ds1 2 (3 3 3))
 ; (ds1 1 (4 3 2))
 ; (ds1 0 (1 3 1))
 ; (ds1 done)

 (define (test-with-output-data-set)
    (define ds-input (mrs:create-data-set))
    (define ds-output (mrs:create-output-data-set))
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 10 value)))
     ds-input
     ds-output)
    (mrs:feed-value-list ds-input '(1 2 3 4)))
  (mrs:run-computation test-with-output-data-set)
 ;;; Note that this returns directly!
 ;Value -> ((1 20) (0 10) (3 40) (2 30))

 (define (test-with-user-specified-callback)
   (define ds-input (mrs:create-data-set))
   (define ds-output (mrs:create-output-data-set))
   (mrs:map
    (lambda (key value)
      (mrs:emit key (* 10 value)))
    ds-input
    ds-output)
   (mrs:feed-value-list ds-input '(1 2 3 4)))
 (mrs:run-computation-with-callback
  test-with-user-specified-callback
  (lambda (k v)
    (pp (list 'user k v))))
 ;;; Printed:
 ; (user 1 20)
 ; (user 2 30)
 ; (user 3 40)
 ; (user 0 10)

 (define (test-multiple-writers-to-output)
    (define ds-input (mrs:create-data-set))
    (define ds-output (mrs:create-output-data-set))
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 10 value)))
     ds-input
     ds-output)
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 11 value)))
     ds-input
     ds-output)
    (mrs:feed-value-list ds-input '(1 2 3 4)))
  (mrs:run-computation test-multiple-writers-to-output)
 ;;; Note that this returns directly!
 ;Value -> ((3 44) (2 33) (1 22) (0 11) (0 10) (3 40) (2 30) (1 20))

 (define (test-file-writer-output-no-dependency)
   (define ds-file-input (mrs:create-data-set))
   (define ds-file-output (mrs:create-file-writer-data-set "output.txt"))
   (define ds-main-input (mrs:create-data-set))
   (define ds-main-output (mrs:create-output-data-set))
   (mrs:map
    (lambda (key value)
      (pp 'file-map)
      (conspire:thread-yield)
      (pp 'emit)
      (mrs:emit key (* 10 value)))
    ds-file-input
    ds-file-output)
   (mrs:map
    (lambda (key value)
      (pp 'main-map)
      (mrs:emit key (* 11 value)))
    ds-main-input
    ds-main-output)
   (mrs:feed-value-list ds-main-input '(1 2 3))
   (mrs:feed-value-list ds-file-input '(1 2 3 4 5 6 7 8 9 10 11 12)))
 (mrs:run-computation test-file-writer-output-no-dependency)

 ;;; Returned
 ;Value 40: ((0 11) (2 33) (1 22))

 ;;; BUT the file output.txt is empty! This is because the main
 ;;; dataflow finished and returned before the file writing was
 ;;; finished. We can fix that by making the done of the main dataflow
 ;;; depend on the completion of the others:


 (define (test-file-writer-output-with-dependency)
   (define ds-file-input (mrs:create-data-set))
   (define ds-file-output (mrs:create-file-writer-data-set "output.txt"))
   (define ds-main-input (mrs:create-data-set))
   (define ds-main-output (mrs:create-output-data-set))
   (mrs:map
    (lambda (key value)
      (conspire:thread-yield)
      (mrs:emit key (* 10 value)))
    ds-file-input
    ds-file-output)
   (mrs:map
    (lambda (key value)
      (mrs:emit key (* 11 value)))
    ds-main-input
    ds-main-output)
   (mrs:depends-on
    ds-file-output
    ds-main-output)
   (mrs:feed-value-list ds-main-input '(1 2 3))
   (mrs:feed-value-list ds-file-input '(1 2 3 4 5 6 7 8 9 10 11 12)))
 (mrs:run-computation test-file-writer-output-with-dependency)
 ;Value 28: ((0 11) (2 33) (1 22))
 ;; Value is returned to the REPL

 ;; And, unlike the test case above, the "output.txt" file contains all
 ;; 12 key-value pairs:
 ; (2 30)
 ; (3 40)
 ; (4 50)
 ; (0 10)
 ; (1 20)
 ; (6 70)
 ; (7 80)
 ; (8 90)
 ; (9 100)
 ; (5 60)
 ; (11 120)
 ; (10 110)


 (define (test-large-data-fact)
   (define ds-fib-input (mrs:create-data-set))
   (define ds-fib-output (mrs:create-file-writer-data-set "output.txt"))
   (mrs:map
    (lambda (key value)
      (mrs:emit key (fact value)))
    ds-fib-input
    ds-fib-output)
   (mrs:feed-value-list ds-fib-input (make-range 500)))
 (mrs:run-computation test-large-data-fact)

 ;;; Works! The output file has the first 500 factorials. They are not
 ;;; in strictly increasing ordering, demonstrating that they are
 ;;; indeed being distributed across our workers.

|#