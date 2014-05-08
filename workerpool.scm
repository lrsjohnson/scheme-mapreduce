(define thread-execute
  (make-generic-operator 1 'thread-execute default-thread-execute))

(define get-pipe-reader
  (make-generic-operator 1 'get-pipe-reader default-get-pipe-reader))

(define get-pipe-writer
  (make-generic-operator 1 'get-pipe-writer default-get-pipe-writer))


;;; Conspire threaded implementation
(define *conspire-threads* #t)

(define (conspire-threads?)
  *conspire-threads*)

(define (make-conspire-pipe)
  (list 'conspire-pipe (queue:make) (conspire:make-lock)))

(define (is-conspire-pipe? pipe)
  (tagged-list? 'conspire-pipe pipe))

(define (conspire-pipe-get-queue pipe)
  (cadr pipe))

(define (conspire-pipe-get-lock pipe)
  (caddr pipe))
  

(defhandler get-pipe-reader
  (lambda (pipe)
    (lambda ()
      (let ((pipe-queue (conspire-pipe-get-queue pipe))
            (pipe-lock (conspire-pipe-get-lock pipe)))
        (let lp ()
          (if (queue:empty? pipe-queue)
              (begin
                (conspire:thread-yield)
                (lp))
              (begin
                (conspire:acquire-lock pipe-lock)
                (if (queue:empty? pipe-queue)
                    (begin
                      (conspire:unlock pipe-lock)
                      (conspire:thread-yield)
                      (lp))
                    (let ((first (queue:get-first pipe-queue)))
                      (queue:delete-from-queue! pipe-queue first)
                      (conspire:unlock pipe-lock)
                      first))))))))
  is-conspire-pipe?)

(defhandler get-pipe-writer
  (lambda (pipe)
    (lambda (value)
      (let ((pipe-queue (conspire-pipe-get-queue pipe))
            (pipe-lock (conspire-pipe-get-lock pipe)))
        (conspire:acquire-lock pipe-lock)
        (queue:add-to-end! pipe-queue value)
        (conspire:unlock pipe-lock))))
  is-conspire-pipe?)

(defhandler thread-execute
  (lambda (thunk)
    (conspire:make-thread conspire:runnable thunk))
  conspire-threads?)

;;; Test conspire implementation
#|
(define (parallel-exec)
  ... ;; Complete test case
|#
