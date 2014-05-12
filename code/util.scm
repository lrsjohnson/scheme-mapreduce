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

(define (fact n)
  (cond ((= n 0) 1)
	(else (* n (fact (- n 1))))))

#|
 (map fact (make-range 10))
 ;Value: (1 1 2 6 24 120 720 5040 40320 362880)
|#

(define (fib n)
  (cond ((< n 2) n)
	(else (+ (fib (- n 1))
		 (fib (- n 2))))))

#|
(map fib (make-range 10))
;Value:: (0 1 1 2 3 5 8 13 21 34)
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

	   
	 