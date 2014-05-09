(define-structure mrq-data-set queue lock)

(define *empty-ds-elt* (list 'empty-ds-elt))

(define (empty-ds-elt? ds-elt)
  (eq? ds-elt *empty-ds-elt*))

(define (create-mrq-data-set)
  (make-mrq-data-set (create-mr-queue) (conspire:make-lock)))
(define create-data-set create-mrq-data-set)

(define ds-get-writer
  (make-generic-operator 1 'ds-get-writer))
(define ds-get-reader
  (make-generic-operator 1 'ds-get-reader))

(defhandler ds-get-writer
  (lambda (mrq-data-set)
    (let ((lock (mrq-data-set-lock mrq-data-set)))
      (lambda (val)
        (conspire:acquire-lock lock)
        (mr-queue-add-elt (mrq-data-set-queue mrq-data-set) val)
        (conspire:unlock lock))))
  mrq-data-set?)

(defhandler ds-get-reader
  (lambda (mrq-data-set)
    (let ((lock (mrq-data-set-lock mrq-data-set))
          (queue (mrq-data-set-queue mrq-data-set)))
      (let ((reader (mr-queue-get-reader queue)))
        (lambda ()
          (conspire:acquire-lock lock)
          (let ((val (reader)))
            (conspire:unlock lock)
            (if (mr-queue-end-val? val)
                *empty-ds-elt*
                val))))))
  mrq-data-set?)

#|
 (define ds (create-mrq-data-set))
 (define writer (ds-get-writer ds))
 (define reader (ds-get-reader ds))
 (define reader2 (ds-get-reader ds))
 (writer 1)
 (writer 2)
 (reader)
 ;-> 1
 (reader2)
 ;-> 1
 (reader2)
 ;-> 2
 (reader)
 ;-> 2
 (reader)
 ;-> (empty-ds-elt)
|# 


(define-structure ds-elt key value done)
(define ds-elt-done? ds-elt-done)

(define (create-ds-elt key value)
  (make-ds-elt key value #f))

(define (create-ds-elt-done)
  (make-ds-elt '() '() #t))

#|
(define ds-elt (create-ds-elt 5 'lars))
(ds-elt-key ds-elt)
;-> 5
(ds-elt-value ds-elt)
;-> lars
(ds-elt-done? ds-elt)
;-> #f
(define ds-elt2 (create-ds-elt-done))
(ds-elt-done? ds-elt2)
;-> #t
|#


(define *sink-data-set* (list 'sink-data-set))
(define (sink-data-set? data-set) (eq? data-set *sink-data-set*))
(define (create-sink-data-set) *sink-data-set*)
;; <3 Lars
(define create-/dev/null-data-set create-sink-data-set)

(defhandler ds-get-reader
  (lambda (sink-data-set)
    (lambda () (error "Cannot read from sink data set")))
  sink-data-set?)

(defhandler ds-get-writer
  (lambda (sink-data-set)
    (lambda (val) 'pass))
  sink-data-set?)