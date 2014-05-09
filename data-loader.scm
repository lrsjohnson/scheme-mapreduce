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

 (define (test1)
   (define ds-input (create-mrq-data-set))
   (define ds1 (create-mrq-data-set))
   (mrs:map
    (lambda (key value)
      (pp value)
      (create-ds-elt key (* 10 value)))
    ds-input
    ds1)
   (mrs:print-streaming ds1)
   (mrs:feed-value-list ds-input '(1 2 3 4))
   (conspire:null-job))
 (with-time-sharing-conspiracy test1)

|#