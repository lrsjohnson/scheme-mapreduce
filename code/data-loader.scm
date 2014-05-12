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
 
