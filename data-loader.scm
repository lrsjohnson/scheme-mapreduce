;; TODO: Make generic? Only works on list right now
(define (mrs:feed-data data-set ds-elt-sequence)
  (let ((writer (ds-get-writer data-set)))
    (for-each (lambda (ds-elt)
                (writer ds-elt))
              ds-elt-sequence)))

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

(define (mrs:feed-done data-set)
  (mrs:feed-data
   data-set
   (list (create-ds-elt-done))))


#|
 (define (test1)
   (define ds-input (create-mrq-data-set))
   (define ds1 (create-mrq-data-set))
   (mrs:print-streaming ds-input 'ds1)
   (mrs:feed-value-list ds-input '(1 2 3 4))
   (conspire:null-job))
 (with-time-sharing-conspiracy test1)

 (define (test2)
   (define ds-input (create-mrq-data-set))
   (define ds1 (create-mrq-data-set))
   (mrs:map
    (lambda (key value)
      (pp value)
      (mrs:emit key (* 10 value)))
    ds-input
    ds1)
   (mrs:print-streaming ds1 'ds1)
   (mrs:feed-value-list ds-input '(1 2 3 4))
   (conspire:null-job))
 (with-time-sharing-conspiracy test2)
;-> (0 10)
;   (1 20)
;   (2 30)
;   (3 40)

(define (test3)
   (define ds-input (create-mrq-data-set))
   (define ds1 (create-mrq-data-set))
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
   (mrs:feed-value-list ds-input '(1 2 3 4))
   (conspire:null-job))
 (with-time-sharing-conspiracy test3)
;-> (0 10)
;   (1 20)
;   (2 30)
;   (3 40)
;   (0 11)
;   (1 22)
;   (2 33)
;   (3 44)


(define (test4)
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
 (with-time-sharing-conspiracy test4)
;-> (ds1 0 10)
;   (ds1 1 20)
;   (ds1 2 30)
;   (ds1 3 40)

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
   (mrs:feed-done ds-input)
   (conspire:null-job))
 (with-time-sharing-conspiracy test5)


|#