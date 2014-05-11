(define (make-range n)
  (let lp ((i (- n 1))
	   (range '()))
    (if (< i 0)
	range
	(lp (- i 1)
	    (cons i range)))))

#|
(make-range 5)
:-> (0 1 2 3 4)
|#

(define (sum lst)
  (reduce + 0 lst))

#|
(sum '(1 3 2))
;-> 6

(sum '(1 2 3 4 5))
;-> 15


(sum '())
;-> 0

|#

	   
	 