;;; User-facing operations
(define (mrs:create-data-set) (create-mrq-data-set))
(define (mrs:create-file-writer-data-set) (create-file-writer-data-set))

;;; General data-set operations
(define *data-sets* '())

(define (register-data-set data-set)
  (set! *data-sets* (cons data-set *data-sets*)))

(define (ds-increment-writer-count data-set)
  (ds-set-writer-count!
   data-set
   (+ 1 (ds-get-writer-count data-set))))

(define (ds-increment-done-count data-set)
  (ds-set-done-count!
   data-set
   (+ 1 (ds-get-done-count data-set))))

(define (flush-input-data-sets)
  (for-each
   (lambda (data-set)
     (if (= (ds-get-writer-count data-set) 0)
	 (ds-add-elt data-set (create-ds-elt-done))))
   *data-sets*))

(define (any? x) #t)

(define (default-ds-get-writer data-set)
  (ds-increment-writer-count data-set)
  (let ((lock (ds-get-lock data-set)))
    (lambda (val)
      (conspire:acquire-lock lock)
      (if (ds-elt-done? val)
          (begin
            (ds-increment-done-count data-set)
            (if (= (ds-get-writer-count data-set)
                   (ds-get-done-count data-set))
                (begin
                  (conspire:unlock lock)
                  (ds-add-elt data-set val))))
          (begin
            (conspire:unlock lock)
            (ds-add-elt data-set val)))
      (conspire:unlock lock))))

;;; Data set generic operators
(define ds-get-writer
  (make-generic-operator 1 'ds-get-writer default-ds-get-writer))
(define ds-get-reader
  (make-generic-operator 1 'ds-get-reader))
(define ds-add-elt
  (make-generic-operator 2 'ds-add-elt))
(define ds-get-writer-count
  (make-generic-operator 1 'ds-get-writer-count))
(define ds-set-writer-count!
  (make-generic-operator 2 'ds-set-writer-count!))
(define ds-get-lock
  (make-generic-operator 1 'ds-get-lock))
(define ds-get-done-count
  (make-generic-operator 1 'ds-get-done-count))
(define ds-set-done-count!
  (make-generic-operator 2 'ds-set-done-count!))


;;; Multi-Reader Queue (Mr. Queue) data sets
(define-structure mrq-data-set queue lock writer-count done-count)

(define (create-mrq-data-set)
  (let ((data-set (make-mrq-data-set
		   (create-mr-queue)
		   (conspire:make-lock) 0 0)))
    (register-data-set data-set)
    data-set))

(defhandler ds-add-elt
  (lambda (mrq-data-set val)
    (let ((lock (mrq-data-set-lock mrq-data-set)))
      (conspire:acquire-lock lock)
      (mr-queue-add-elt (mrq-data-set-queue mrq-data-set) val)
      (conspire:unlock lock)))
  mrq-data-set? any?)

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

(defhandler ds-get-writer-count
  mrq-data-set-writer-count
  mrq-data-set?)

(defhandler ds-set-writer-count!
  set-mrq-data-set-writer-count!
  mrq-data-set?)

(defhandler ds-get-done-count
  mrq-data-set-done-count
  mrq-data-set?)

(defhandler ds-set-done-count!
  set-mrq-data-set-done-count!
  mrq-data-set?)

(defhandler ds-get-lock
  mrq-data-set-lock
  mrq-data-set?)

#|
 (define (test-mrq)
   (define ds (create-mrq-data-set))
   (define writer (ds-get-writer ds))
   (define reader (ds-get-reader ds))
   (define reader2 (ds-get-reader ds))
   (writer 1)
   (writer 2)
   (pp (reader))
   (pp (reader2))
   (pp (reader2))
   (pp (reader))
   (pp (reader)))
 (with-time-sharing-conspiracy test-mrq)
 ;-> 1
 ;   1
 ;   2
 ;   2
 ;   (empty-ds-elt)

 (define (test-mrq-multi-writer)
   (define ds2 (create-mrq-data-set))
   (define writer (ds-get-writer ds2))
   (define writer2 (ds-get-writer ds2))
   (define reader (ds-get-reader ds2))

   (writer 1)
   (writer2 2)
   (writer2 (create-ds-elt-done))
   (pp (reader))
   (pp (reader))
   (pp (reader))

   (writer (create-ds-elt-done))

   (pp (reader)))
 (with-time-sharing-conspiracy test-mrq-multi-writer)
 ;-> 1
 ;-> 2
 ;-> (empty-ds-elt)
 ;-> [done element]
|#

;;; File writer data sets
(define-structure file-writer-data-set file done lock writer-count done-count)

(define (create-file-writer-data-set filename)
  (let* ((file (open-output-file filename))
         (data-set (make-file-writer-data-set file #f (conspire:make-lock) 0 0)))
    (register-data-set data-set)
    data-set))

(defhandler ds-get-reader
  (lambda (data-set)
    (lambda ()
      (if (file-writer-data-set-done data-set)
          (begin
            (create-ds-elt-done)
            (set-file-writer-data-set-done! data-set #f))
          *empty-ds-elt*)))
  file-writer-data-set?)

(define (file-writer-data-set-add-elt data-set elt)
  (let ((lock (file-writer-data-set-lock data-set))
        (file (file-writer-data-set-file data-set)))
    (if (not (ds-elt-done? elt))
        (begin
          (conspire:acquire-lock lock)
          (write (list (ds-elt-key elt) (ds-elt-value elt)) file)
          (newline file)
          (flush-output file)
          (conspire:unlock lock))
        (set-file-writer-data-set-done! data-set #t))))
  
(defhandler ds-add-elt
  file-writer-data-set-add-elt
  file-writer-data-set?)

(defhandler ds-get-writer-count
  file-writer-data-set-writer-count
  file-writer-data-set?)

(defhandler ds-set-writer-count!
  set-file-writer-data-set-writer-count!
  file-writer-data-set?)

(defhandler ds-get-done-count
  file-writer-data-set-done-count
  file-writer-data-set?)

(defhandler ds-set-done-count!
  set-file-writer-data-set-done-count!
  file-writer-data-set?)

(defhandler ds-get-lock
  file-writer-data-set-lock
  file-writer-data-set?)

#|
 (define (test-file-writer)
   (define ds-file (create-file-writer-data-set "output.txt"))
   (mrs:feed-value-list ds-file '(a b c d)))
 (mrs:run-computation test-file-writer)
 ;; Contents of output.txt:
 ;-> (0 a)
 ;   (1 b)
 ;   (2 c)
 ;   (3 d)
|#


;;; Sink data set
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

(defhandler ds-get-writer-count
  (lambda (data-set)
    (error "Writer count does not apply to /dev/null"))
  sink-data-set?)

(defhandler ds-set-writer-count!
  (lambda (data-set writer-count)
    (error "Writer count does not apply to /dev/null"))
  sink-data-set?)


;;; Data-set elements
(define-structure ds-elt key value done)
(define (ds-elt-done? obj)
  (and (ds-elt? obj)
       (ds-elt-done obj)))

(define (create-ds-elt key value)
  (make-ds-elt key value #f))

(define (create-ds-elt-done)
  (make-ds-elt '() '() #t))

(define *empty-ds-elt* (list 'empty-ds-elt))

(define (empty-ds-elt? ds-elt)
  (eq? ds-elt *empty-ds-elt*))


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