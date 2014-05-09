;;; Kind of gross, but we need to define this somewhere for the fluid-let
;;; operations below
(define (mrs:emit key value) (error "mrs:emit undefined for this operation"))

;;; Map (actually a multi-map)
;;; Requires a function from key,value which calls emit on key.value
;;; pairs to output.
(define (mrs:map multi-map-func ds-in ds-out)
  (define (mm-func kv-emit)
      (lambda (ds-elt)
        (if (not (ds-elt-done? ds-elt))
            (fluid-let ((mrs:emit
                         (lambda (key value)
                           (kv-emit (create-ds-elt key value)))))
              (multi-map-func (ds-elt-key ds-elt) (ds-elt-value ds-elt)))
            (kv-emit ds-elt))))
  (make-distributor mm-func ds-in ds-out 5))

;;; Filter
;;; Requires a function from key,value to #t/#f
(define (mrs:filter filter-func ds-in ds-out)
  (define (mm-func emit)
    (lambda (ds-elt)
      (if (not (ds-elt-done? ds-elt))
          (if (filter-func (ds-elt-key ds-elt) (ds-elt-value ds-elt))
              (emit ds-elt))
          (emit ds-elt))))
  (make-distributor mm-func ds-in ds-out 5))

;;; Aggregate
(define (append-data-in-hashtable hash-table key value)
  (hash-table/put!
   hash-table
   key
   (cons value (hash-table/get hash-table key '()))))
(define (mrs:aggregate ds-in ds-out)
  (define (mm-func emit)
    (let ((data-from-mapper (make-equal-hash-table)))
      (lambda (ds-elt)
        (if (not (ds-elt-done? ds-elt))
            (append-date-in-hashtable data-from-mapper key value)
            (hash-table/for-each
             data-from-mapper
             (lambda (key values)
               (emit (create-ds-elt key values))
               (emit ds-elt))))))
    (make-distributor mm-func ds-in ds-out 1)))

;;; Reduce
(define (mrs:reduce reduce-func ds-in ds-out)
  (define ds-intermediate (create-data-set))
  (mrs:aggregate ds-in ds-intermediate)
  (mrs:map reduce-func ds-intermediate ds-out))
      

(define (mrs:print-streaming ds-in tag)
  (define (mm-func emit)
    (lambda (ds-elt)
      (if (ds-elt-done? ds-elt)
          (pp `(,tag done))
          (pp `(,tag ,(ds-elt-key ds-elt) ,(ds-elt-value ds-elt))))))
  (make-distributor mm-func ds-in (create-sink-data-set) 1))