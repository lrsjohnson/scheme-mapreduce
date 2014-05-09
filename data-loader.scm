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

#|
 (define (test1)
   (define ds-input (create-mrq-data-set))
   (define ds1 (create-mrq-data-set))
   (mrs:print-streaming ds-input)
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
   (mrs:print-streaming ds1)
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
   (mrs:print-streaming ds1)
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
|#