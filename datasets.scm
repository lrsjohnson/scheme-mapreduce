(define-structure data-set queue lock)

(define *empty-ds-elt* (list 'empty-ds-elt))

(define (empty-ds-elt? ds-elt)
  (eq? ds-elt *empty-ds-elt*))

(define (create-data-set)
  (make-data-set (create-mr-queue) (conspire:make-lock)))

(define (ds-get-writer data-set)
  (let ((lock (data-set-lock data-set)))
    (lambda (val)
      (conspire:acquire-lock lock)
      (mr-queue-add-elt (data-set-queue data-set) val)
      (conspire:unlock lock))))

(define (ds-get-reader data-set)
  (let ((lock (data-set-lock data-set))
        (queue (data-set-queue data-set)))
    (let ((reader (mr-queue-get-reader queue)))
      (lambda ()
        (conspire:acquire-lock lock)
        (let ((val (reader)))
          (conspire:unlock lock)
          (if (mr-queue-end-val? val)
              *empty-ds-elt*
              val))))))

#|
 (define ds (create-data-set))
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