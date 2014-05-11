;;; Interfaces to read and write procedures / lists from ports
;;;

(define (procedure->string proc)
  (with-output-to-string (lambda () (pp proc))))

(define (string->scheme-list str)
  (with-input-from-string str read))

(define (eval-code procedure-code)
  (eval procedure-code (nearest-repl/environment)))

#|
 (define (f x)
   (+ x 10))
 ;-> f

 (define f-string (procedure->string f))
 ;-> f-string

 f-string
 ;-> "(named-lambda (f x)\n  (+ x 10))\n"

 (define f-proc-code (string->scheme-list f-string))
 ;-> f-proc-code

 f-proc-code
 ;-> (named-lambda (f x) (+ x 10))

 (define new-f (eval-code f-proc-code))
 ;-> new-f

 (new-f 5)
 ;-> 15

|#

