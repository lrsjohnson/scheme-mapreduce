;;; @needsconspiracy
;;; Makes and starts a thread which...
;;; Reads ds-elements from input-reader, applies
;;; mm-func, which calls output-writer with emitted values.
;;; Returns worker (reader, writer)
(define (make-worker mm-func)
  (let ((output-pipe (make-conspire-pipe))
        (input-pipe (make-conspire-pipe)))
    (let ((output-writer (get-pipe-writer output-pipe))
          (input-reader (get-pipe-reader input-pipe)))
      (pp 'make-worker)
      (conspire:make-thread
       conspire:runnable
       (lambda ()
         (let ((worker-func (mm-func output-writer)))
           (let lp ()
             (let ((input-val (input-reader)))
               (if (not (empty-pipe-val? input-val))
                   (worker-func input-val))
               (lp)))))))
    (list (get-pipe-reader output-pipe)
          (get-pipe-writer input-pipe))))

(define (get-worker-reader worker)
  (car worker))
(define (get-worker-writer worker)
  (cadr worker))

;;; Makes num-workers workers, returning a vector of size
;;; num-workers containing pairs of input and output pipes
;;; for each worker
(define (make-workers mm-func num-workers)
  (let ((workers (make-vector num-workers)))
    (let lp ((i 0))
      (vector-set! workers i (make-worker mm-func))
      (set! i (+ i 1))
      (if (< i num-workers)
          (lp i)))
    workers))

;;; Return worker index to distribute ds-elt to
(define (choose-worker ds-elt workers)
  (vector-ref
   workers
   (remainder (hash ds-elt) (vector-length workers))))

(define (write-to-worker worker ds-elt)
  ((get-worker-writer worker) ds-elt))

(define (read-from-worker worker)
  ((get-worker-reader worker)))

;;; @needsconspiracy
;;; Sets up workers, then starts a thread which...
;;; Loops over input ds and worker outputs and passes
;;; values to appropriate destinations (using black magic).
(define (make-distributor mm-func ds-in ds-out num-workers)
  (let ((workers (make-workers mm-func num-workers))
        (ds-in-reader (ds-get-reader ds-in))
        (ds-out-writer (ds-get-writer ds-out)))
    (pp 'make-distributor)
    (conspire:make-thread
     conspire:runnable
     (lambda ()
       (let lp ()
         (let ((ds-in-elt (ds-in-reader)))
           (cond ((empty-ds-elt? ds-in-elt)
                  'continue)
                 ((ds-elt-done? ds-in-elt)
                  (error "Implement me!"))
                 (else
                  (write-to-worker
                   (choose-worker ds-in-elt workers)
                   ds-in-elt))))
         (vector-for-each
          (lambda (worker)
            (let ((worker-output (read-from-worker worker)))
              (if (not (empty-pipe-val? worker-output))
                  (ds-out-writer worker-output))))
          workers)
         (lp))))))