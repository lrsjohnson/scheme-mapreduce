(define (make-server port-num)
  (let ((server-socket (open-tcp-server-socket port-num)))
    (pp 'server-socket-created)
    (let ((incoming-port (tcp-server-connection-accept
			  server-socket
			  #t ;; Block on Port
			  #f)))
      (pp 'connection-accepted)
      (let lp ((c-in (read-line incoming-port)))
	(pp (list 'sever port-num c-in))
	(lp (read-line incoming-port))))))

(make-server 8989)