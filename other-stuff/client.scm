
(define (make-client port-num)
  (let ((socket (open-tcp-stream-socket "localhost" port-num)))
    (write-string "Hello\n" socket)
    (flush-output socket)
    socket))

(define c (make-client 9000))

(write-string "Another Message\n" c)

