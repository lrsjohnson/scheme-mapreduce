;; TODO: Make generic? Only works on list right now
(define (mrs:feed-data data-set ds-elt-sequence)
  (pp 'feed-data)
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

(define (mrs:run-computation thunk)
  (with-time-sharing-conspiracy
   (lambda ()
     (thunk)
     (pp 'run-c)
     (flush-input-data-sets)
     (conspire:null-job))))

#|
 (define (test1)
   (define ds-input (create-data-set))
   (define ds1 (create-data-set))
   (mrs:print-streaming ds-input 'ds1)
   (mrs:feed-value-list ds-input '(1 2 3 4)))
 (mrs:run-computation test1)

 (define (test2)
   (define ds-input (create-data-set))
   (define ds1 (create-data-set))
   (mrs:map
    (lambda (key value)
      (pp value)
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
   (define ds-input (create-data-set))
   (define ds1 (create-data-set))
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
   (define ds-input (create-data-set))
   (define ds1 (create-data-set))
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
   (define ds-input (create-data-set))
   (define ds1 (create-data-set))
   (define ds2 (create-data-set))
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
   (mrs:feed-value-list ds-input '(1 2 3 4))
   (conspire:null-job))
 (with-time-sharing-conspiracy test5)
|#