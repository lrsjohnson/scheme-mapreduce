;; TODO: Make generic? Only works on list right now
(define (mrs:feed-data data-set ds-elt-sequence)
  (for-each (lambda (ds-elt)
	      (ds-add-elt data-set ds-elt))
              ds-elt-sequence))

(define (mrs:feed-value-list data-set value-list)
  (mrs:feed-data
   data-set
   (let lp ((i 0)
            (value-list value-list))
     (if (null? value-list)
	 '()
         (cons (create-ds-elt i (car value-list))
	       (lp (+ i 1)
		   (cdr value-list)))))))

(define (mrs:feed-key-value-list data-set key-value-list)
  (mrs:feed-data
   data-set
   (map (lambda (kv)
          (create-ds-elt (car kv) (cadr kv))) key-value-list)))

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
  (define (test1)
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (mrs:print-streaming ds-input 'ds1)
    (mrs:feed-value-list ds-input '(1 2 3 4)))
  (mrs:run-computation test1)
 ;->  (ds1 0 1)
 ;    (ds1 1 2)
 ;    (ds1 2 3)
 ;    (ds1 3 4)
 ;    (ds1 done)

  (define (test2)
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (mrs:map
     (lambda (key value)
       (mrs:emit key (* 10 value)))
     ds-input
     ds1)
    (mrs:print-streaming ds1 'ds1)
    (mrs:feed-value-list ds-input '(1 2 3 4)))
  (mrs:run-computation test2)
 ;   (ds1 3 40)
 ;   (ds1 0 10)
 ;   (ds1 1 20)
 ;   (ds1 2 30)
 ;   (ds1 done)

 (define (test3)
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
  (mrs:run-computation test3)
 ;  (ds1 1 20)
 ;  (ds1 2 30)
 ;  (ds1 3 40)
 ;  (ds1 0 10)
 ;  (ds1 1 22)
 ;  (ds1 2 33)
 ;  (ds1 3 44)
 ;  (ds1 0 11)
 ;  (ds1 done)

  (define (test4)
    (define ds-input (mrs:create-data-set))
    (define ds1 (mrs:create-data-set))
    (mrs:aggregate
     ds-input
     ds1)
    (mrs:print-streaming ds1 'ds1)
    (mrs:feed-value-list ds-input '(1 2 3 4))
    (mrs:feed-value-list ds-input '(3 3 3 3))
    (mrs:feed-value-list ds-input '(1 4 3 4)))
  (mrs:run-computation test4)
 ; (ds1 3 (4 3 4))
 ; (ds1 2 (3 3 3))
 ; (ds1 1 (4 3 2))
 ; (ds1 0 (1 3 1))
 ; (ds1 done)

 (define (test5)
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
  (mrs:run-computation test5)
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
 ;-> ((1 20) (0 10) (3 40) (2 30))
|#