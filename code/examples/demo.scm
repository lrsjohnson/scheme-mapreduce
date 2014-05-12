;;; Flow computation demo
(define (demo)
  (define 2d-vectors (mrs:create-data-set))
  (define 3d-vectors (mrs:create-data-set))
  (define magnitudes (mrs:create-data-set))
  (define in-out-flow (mrs:create-output-data-set))
  (mrs:map
   (lambda (key 2d-vec)
     (let ((x (car 2d-vec)) (y (cadr 2d-vec)))
       (mrs:emit key (sqrt (+ (* x x) (* y y))))))
   2d-vectors
   magnitudes)
  (mrs:map
   (lambda (key 3d-vec)
     (let ((x (car 3d-vec)) (y (cadr 3d-vec)) (z (caddr 3d-vec)))
       (mrs:emit key (sqrt (+ (* x x) (* y y) (* z z))))))
   3d-vectors
   magnitudes)
  (mrs:reduce
   (lambda (key values)
     (mrs:emit key (apply + values)))
   magnitudes
   in-out-flow)
  (mrs:feed-key-value-list 2d-vectors '((in (1 2))
                                        (in (3 3))
                                        (out (5 0))))
  (mrs:feed-key-value-list 3d-vectors '((in (1 1 1))
                                        (out (5 0 0)))))
(mrs:run-computation demo)