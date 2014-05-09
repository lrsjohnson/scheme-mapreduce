;;; Kind of gross, but we need to define this somewhere for the fluid-let
;;; operations below
(define (mrs:emit key value) (error "mrs:emit undefined for this operation"))

;;; Map (actually a multi-map)
;;; Requires a function from key,value which calls emit on key.value
;;; pairs to output.
(define (mrs:map multi-map-func ds-in ds-out)
  (define (mm-func kv-emit)
      (lambda (ds-elt)
        (fluid-let ((mrs:emit
                     (lambda (key value)
                       (kv-emit (create-ds-elt key value)))))
          (multi-map-func (ds-elt-key ds-elt) (ds-elt-value ds-elt)))))
  (make-distributor mm-func ds-in ds-out 5))

;;; Filter
;;; Requires a function from key,value to #t/#f
(define (mrs:filter filter-func ds-in ds-out)
  (define (mm-func emit)
    (lambda (ds-elt)
      (if (filter-func (ds-elt-key ds-elt) (ds-elt-value ds-elt))
          (emit ds-elt))))
  (make-distributor mm-func ds-in ds-out 5))

;;; 

(define (mrs:print-streaming ds-in)
  (define (mm-func emit)
    (lambda (ds-elt)
      (pp `(,(ds-elt-key ds-elt) ,(ds-elt-value ds-elt)))))
  (make-distributor mm-func ds-in (create-sink-data-set) 1))