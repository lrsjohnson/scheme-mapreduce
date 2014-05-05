;;;; The famous Samefringe problem:
;;;    Do two finite trees have the same fringe?


;;;             On The Fringes of Fun with Control Structures

;;; Let's look at a variety of ways to solve a famous problem, the classic
;;; "same fringe" problem.

;;; The "fringe" of a tree is defined to be the ordered list of terminal
;;; leaves of the tree encountered when the tree is traversed in some
;;; standard order, say depth-first left-to-right.  We can easily compute
;;; the fringe of a tree represented as a list structure.  In the programs
;;; that follow we add an explicit test to exclude the empty list from the
;;; answer:

#|
     (define (fringe subtree)
       (cond ((pair? subtree)
	      (append (fringe (car subtree))
		      (fringe (cdr subtree))))
	     ((null? subtree) '())
	     (else (list subtree))))

;;; Where append is usually defined as:

     (define (append l1 l2)
       (if (pair? l1)
	   (cons (car l1)
		 (append (cdr l1) l2))
	   l2))
|#

;;; So the fringe of a typical tree is:

#|
     (fringe '((a b) c ((d)) e (f ((g h)))))
     ;Value: (a b c d e f g h)

     (fringe '(a b c ((d) () e) (f (g (h)))))
     ;Value: (a b c d e f g h)
|#

;;; That was a horribly inefficient computation, because append keeps
;;; copying parts of the fringe over and over.

;;; Here is a nicer procedure that computes the fringe, without any
;;; nasty re-copying.

(define (fringe subtree)
  (define (walk subtree ans)
    (cond ((pair? subtree)
	   (walk (car subtree)
		 (walk (cdr subtree)
		       ans)))
	  ((null? subtree) ans)
	  (else (cons subtree ans))))
  (walk subtree '()))

;;; So the "same fringe" problem appears really simple:

(define (same-fringe? tree1 tree2)
  (equal? (fringe tree1) (fringe tree2)))

;;; Indeed, this works:

#|
(same-fringe? '((a b) c ((d)) e (f ((g h))))
	      '(a b c ((d) () e) (f (g (h)))))
;Value: #t

(same-fringe? '((a b) c ((d)) e (f ((g h))))
	      '(a b c ((d) () e) (g (f (h)))))
;Value: #f
|#

;;; Unfortunately, this requires computing the entire fringe of each tree
;;; before comparing the fringes.  Suppose that the trees were very big,
;;; but that they were likely to differ early in the fringe.  This would
;;; be a terrible strategy.  We would rather have a way of generating the
;;; next element of the fringe of each tree only as needed to compare them.

;;; One way to do this is with "lazy evaluation", (using Scheme streams).
;;; This method requires examining only as much of the input trees as is
;;; necessary to decide when two fringes are not the same:

(define (lazy-fringe subtree)
  (cond ((pair? subtree)
	 (stream-append-deferred (lazy-fringe (car subtree))
				 (lambda ()
				   (lazy-fringe (cdr subtree)))))
	((null? subtree) the-empty-stream)
	(else (stream subtree))))

(define (lazy-same-fringe? tree1 tree2)
  (let lp ((f1 (lazy-fringe tree1))
	   (f2 (lazy-fringe tree2)))
    (cond ((and (stream-null? f1) (stream-null? f2)) #t)
	  ((or  (stream-null? f1) (stream-null? f2)) #f)
	  ((eq? (stream-car   f1) (stream-car   f2))
	   (lp  (stream-cdr   f1) (stream-cdr   f2)))
	  (else #f))))

(define (stream-append-deferred stream1 stream2-thunk)
  (if (stream-pair? stream1)
      (cons-stream (stream-car stream1)
		   (stream-append-deferred (stream-cdr stream1)
					   stream2-thunk))
      (stream2-thunk)))

(define the-empty-stream (stream))

#|
(lazy-same-fringe?
 '((a b) c ((d)) e (f ((g h))))
 '(a b c ((d) () e) (f (g (h)))))
;Value: #t

(lazy-same-fringe?
 '((a b) c ((d)) e (f ((g h))))
 '(a b c ((d) () e) (g (f (h)))))
;Value: #f
|#

;;; An alternative incremental idea is to make coroutines that generate
;;; the fringes, using an explicit continuation argument and local state.

;;; Notice that in the following code we invent a special object *done*.
;;; Because it is a newly consed list, this object is eq? only to itself,
;;; so it cannot be confused with any other object.  This is a very common
;;; device for making unique objects.

(define *done* (list '*done*))

(define (coroutine-fringe-generator tree)
  (define (resume-thunk)
    (walk tree (lambda () *done*)))
  (define (walk subtree continue)
    (cond ((null? subtree)
	   (continue))
	  ((pair? subtree)
	   (walk (car subtree)
		 (lambda ()
		   (walk (cdr subtree)
			 continue))))
	  (else
	   (set! resume-thunk continue)
	   subtree)))
  (lambda () (resume-thunk)))

;;; Why is it necessary to use the expression "(lambda () (resume-thunk))"
;;; rather than just "resume-thunk" as the returned value of the fringe
;;; generator?  Aren't they the same, by the eta rule of lambda calculus?

(define (coroutine-same-fringe? tree1 tree2)
  (let ((f1 (coroutine-fringe-generator tree1))
	(f2 (coroutine-fringe-generator tree2)))
    (let lp ((x1 (f1)) (x2 (f2)))
      (cond ((and (eq? x1 *done*) (eq? x2 *done*)) #t)
	    ((or  (eq? x1 *done*) (eq? x2 *done*)) #f)
	    ((eq? x1 x2) (lp (f1) (f2)))
	    (else #f)))))

;;; Also notice the peculiar SET! assignment in this code.  This makes it
;;; possible for the procedures f1 and f2 (two distinct results of calling
;;; the fringe generator) to maintain independent resume continuations
;;; each time they are re-invoked to proceed generating their fringes.
;;; This assignment is what gives each new fringe generator its own
;;; dynamic local state.

#|
(coroutine-same-fringe?
 '((a b) c ((d)) e (f ((g h))))
 '(a b c ((d) () e) (f (g (h)))))
;Value: #t

(coroutine-same-fringe?
 '((a b) c ((d)) e (f ((g h))))
 '(a b c ((d) () e) (g (f (h)))))
;Value: #f
|#

;;; We can abstract this control structure, using continuations.  Now
;;; things get very complicated.  Here, a procedure that is to be used
;;; as a coroutine takes an argument: return.  Its value is a thunk
;;; that can be called to start the coroutine computing.

;;; This is a coroutine manager that takes a todo procedure and
;;; returns a receiver, When the receiver is invoked with a supplicant
;;; continuation the coroutine manager gives the todo procedure a
;;; return procedure to call if and when it gets a value worth
;;; returning from the receiver.  The return procedure collects the
;;; the continuation of the todo so that next time the receiver is
;;; invoked the todo will be resumed where it left off, with a return
;;; from the new call of the receiver.

(define (make-coroutine todo)
  (let ((continue #f))
    (lambda (supplicant)
      (let ((resume-point supplicant))
	(define (return value)
	  (set! resume-point
		(call-with-current-continuation
		 (lambda (k)
		   (set! continue k)
		   (resume-point value)))))
	(if continue
	    (continue supplicant)
	    (todo return))))))


;;; Amazingly!

(define next-value
  call-with-current-continuation)

(define *done* (list '*done*))

(define (done? x) (eq? x *done*))

(define (list-iterator list)
  (make-coroutine 
   (lambda (return) 
     (for-each return list)
     (return *done*))))

#|
(define foo (list-iterator '(1 2 3)))

(next-value foo)
;Value: 1

(next-value foo)
;Value: 2

(next-value foo)
;Value: 3

(next-value foo)
;Value: (*done*)
|#

#|
(let ((foo (list-iterator '(1 2 3))))
  (let lp ((v (next-value foo)))
    (if (not (done? v))
	(begin (pp v)
	       (lp (next-value foo)))
	'done)))
1
2
3
;Value: done

(let ((foo (list-iterator '(1 2 3)))
      (bar (list-iterator '(a b c))))
  (let lp ((v (next-value foo))
	   (w (next-value bar)))
    (pp (list v w))
    (if (and (not (done? v)) (not (done? w)))
	(lp (next-value foo)
	    (next-value bar))
	'done)))
(1 a)
(2 b)
(3 c)
((*done*) (*done*))
;Value: done
|#

(define (fringe-generator tree)
  (make-coroutine
   (lambda (return)
     (define (walk subtree)
       (cond ((pair? subtree)
	      (walk (car subtree))
	      (walk (cdr subtree)))
	     ((null? subtree) 'goop)
	     (else (return subtree))))
     (walk tree)
     (return *done*))))

#|
(define foo (fringe-generator '(1 (2 3) 4)))

(next-value foo)
;Value: 1

(next-value foo)
;Value: 2

(next-value foo)
;Value: 3

(next-value foo)
;Value: 4

(next-value foo)
;Value: (*done*)
|#

(define (same-fringe? tree1 tree2)
  (let ((f1 (fringe-generator tree1))
	(f2 (fringe-generator tree2)))
    (let lp ((x1 (next-value f1))
	     (x2 (next-value f2)))
      ;; (pp (list x1 x2))
      (cond ((and (done? x1) (done? x2))
	     #t)
	    ((or (done? x1) (done? x2))
	     #f)
	    ((eq? x1 x2)
	     (lp (next-value f1)
		 (next-value f2)))
	    (else #f)))))
#|
(same-fringe?
 '((a b) c ((d)) e (f ((g h))))
 '(a b c ((d) () e) (f (g (h)))))
;Value: #t

(same-fringe?
 '((a b) c ((d)) e (f ((g h))))
 '(a b c ((d) () e) (g (f (h)))))
;Value: #f

(same-fringe?
 '((a b) c ((d)) e (f ((g h))))
 '(a b c ((d) () e) (g (f ))))
;Value: #f
|#

;;;                      Communication among Threads

;;; Now that we are all warmed up about continuations, you are ready to
;;; look at the time-sharing thread code in "conspire.scm", and the
;;; parallel execution code in "try-two-ways.scm".  The time-sharing
;;; monitor can easily implement coroutines.  You have an example with an
;;; explicit thread-yield in the first simple example in "conspire.scm".
;;; The return procedure above can be thought of as a thread yield.
;;; However, the coroutines in the time-shared environment do not easily
;;; communicate except through shared variables.

;;; Time-sharing systems, such as GNU/Linux, provide explicit mechanisms,
;;; such as pipes, to make it easy for processes to communicate.  A pipe is
;;; basically a FIFO communication channel which provides a reader and a
;;; writer.  The writer puts things into the pipe and the reader takes
;;; them out.  If we had pipes in conspire we could write the same-fringe?
;;; program as follows:

(define *done* (list '*done*))

(define (piped-same-fringe? tree1 tree2)
  (let ((p1 (make-pipe)) (p2 (make-pipe)))
    (let ((thread1
	   (conspire:make-thread
	    conspire:runnable
	    (lambda ()
	      (piped-fringe-generator tree1 (pipe-writer p1)))))
	  (thread2
	   (conspire:make-thread
	    conspire:runnable
	    (lambda ()
	      (piped-fringe-generator tree2 (pipe-writer p2)))))
	  (f1 (pipe-reader p1))
	  (f2 (pipe-reader p2)))
      (let lp ((x1 (f1)) (x2 (f2)))
	(cond ((and (eq? x1 *done*) (eq? x2 *done*)) #t)
	      ((or  (eq? x1 *done*) (eq? x2 *done*)) #f)
	      ((eq? x1 x2) (lp (f1) (f2)))
	      (else #f))))))

(define (piped-fringe-generator tree return)
  (define (lp tree)
    (cond ((pair? tree)
	   (lp (car tree))
	   (lp (cdr tree)))
	  ((null? tree) unspecific)
	  (else
	   (return tree))))
  (lp tree)
  (return *done*))

#|
(with-time-sharing-conspiracy
 (lambda ()
   (piped-same-fringe?
    '((a b) c ((d)) e (f ((g h))))
    '(a b c ((d) () e) (f (g (h)))))
   ))
;Value: #t

(with-time-sharing-conspiracy
 (lambda ()
   (piped-same-fringe?
    '((a b) c ((d)) e (f ((g h))))
    '(a b c ((d) () e) (g (f (h)))))
   ))
;Value: #f

(with-time-sharing-conspiracy
 (lambda ()
   (piped-same-fringe?
    '((a b) c ((d)) e (f ((g h))))
    '(a b c ((d) () e) (g (f ))))
   ))
;Value: #f
|#

;;; With appropriate abstraction we can make the program look almost
;;; exactly the same as the coroutine version:

(define *done* (list '*done*))

(define (tf-piped-same-fringe? tree1 tree2)
  (let ((f1 (make-threaded-filter (tf-piped-fringe-generator tree1)))
	(f2 (make-threaded-filter (tf-piped-fringe-generator tree2))))
    (let lp ((x1 (f1)) (x2 (f2)))
      (cond ((and (eq? x1 *done*) (eq? x2 *done*)) #t)
	    ((or  (eq? x1 *done*) (eq? x2 *done*)) #f)
	    ((eq? x1 x2) (lp (f1) (f2)))
	    (else #f)))))

(define (tf-piped-fringe-generator tree)
  (lambda (return)
    (define (lp tree)
      (cond ((pair? tree)
	     (lp (car tree))
	     (lp (cdr tree)))
	    ((null? tree) unspecific)
	    (else
	     (return tree))))
    (lp tree)
    (return *done*)))

#|
(with-time-sharing-conspiracy
 (lambda ()
   (tf-piped-same-fringe?
    '((a b) c ((d)) e (f ((g h))))
    '(a b c ((d) () e) (f (g (h)))))
   ))
;Value: #t

(with-time-sharing-conspiracy
 (lambda ()
   (tf-piped-same-fringe?
    '((a b) c ((d)) e (f ((g h))))
    '(a b c ((d) () e) (g (f (h)))))
   ))
;Value: #f

(with-time-sharing-conspiracy
 (lambda ()
   (tf-piped-same-fringe?
    '((a b) c ((d)) e (f ((g h))))
    '(a b c ((d) () e) (g (f ))))
   ))
;Value: #f
|#
