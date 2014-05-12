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
            (append-data-in-hashtable
	     data-from-mapper
	     (ds-elt-key ds-elt)
	     (ds-elt-value ds-elt))
            (begin (hash-table/for-each
		    data-from-mapper
		    (lambda (key values)
		      (emit (create-ds-elt key values))))
		   (set! data-from-mapper (make-equal-hash-table))
		   (emit ds-elt))
		   ))))
  (make-distributor mm-func ds-in ds-out 1))

;;; Reduce
(define (mrs:reduce reduce-func ds-in ds-out)
  (define ds-intermediate (mrs:create-data-set))
  (mrs:aggregate ds-in ds-intermediate)
  (mrs:map reduce-func ds-intermediate ds-out))
      

(define (mrs:print-streaming ds-in tag)
  (define (mm-func emit)
    (lambda (ds-elt)
      (if (ds-elt-done? ds-elt)
          (pp `(,tag done))
          (pp `(,tag ,(ds-elt-key ds-elt) ,(ds-elt-value ds-elt))))))
  (make-distributor mm-func ds-in (mrs:create-sink-data-set) 1))

;;; ds-out only receives a done once ds-in is done
;;; Example use case: User specifies return output data
;;; set depends on file output data sets, so entire file is
;;; written out before returning to the user
(define (mrs:depends-on ds-in ds-out)
  (define (mm-func emit)
    (lambda (ds-elt)
      (if (ds-elt-done? ds-elt)
	  (begin
	    (pp 'depends-on-done)
	    (emit ds-elt)))))
  (make-distributor mm-func ds-in ds-out 1))


;;; Constructor-style operations
(define (make-constructor-operation operation)
  (define (new-op . args)
    (define ds-out (create-data-set))
    (apply operation (append args (list ds-out)))
    ds-out)
  new-op)

(define mrs:c-map (make-constructor-operation mrs:map))
(define mrs:c-filter (make-constructor-operation mrs:filter))
(define mrs:c-aggregate (make-constructor-operation mrs:aggregate))
(define mrs:c-reduce (make-constructor-operation mrs:reduce))

#|
(define (test1)
  (define ds-input (create-data-set))
  (define ds-out
    (mrs:c-map
     (lambda (key value)
       (mrs:emit key (* 10 value)))
     ds-input))
  (mrs:print-streaming ds-out 'out)
  (mrs:feed-value-list ds-input '(1 2 3 4)))
(mrs:run-computation test1)
;-> (out 0 10)
;   (out 1 20)
;   (out 2 30)
;   (out 3 40)
;   (out done)

(define (test-depends-on)
  (define ds-input (create-data-set))
  (define ds-sink (create-data-set))
  (define ds-done-check (create-data-set))
  (mrs:map (lambda (k v)
             (pp (list k v))) ds-input ds-sink)
  (mrs:depends-on ds-sink ds-done-check)
  (mrs:feed-value-list ds-input '(1 2 3 4))
  (mrs:print-streaming ds-done-check 'out))
(mrs:run-computation test-depends-on)
;-> (3 4)
;   (0 1)
;   (1 2)
;   (2 3)
;   (out done) ;; Happens afterwards
|#