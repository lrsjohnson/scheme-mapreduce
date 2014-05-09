(define-structure mr-queue element-list end-ptr deleted-count reader-infos)
(define-structure reader-info ptr index)

(define *mr-queue-end* (list 'mr-queue-end))
(define (make-mr-queue-end) (list *mr-queue-end*))
(define (mr-queue-end-val? val) (eq? *mr-queue-end* val))

(define (make-mr-queue)
  (let ((element-list (make-mr-queue-end)))
    (list 'mr-queue element-list element-list 0 '())))

(define (mr-queue-add-elt mr-queue elt)
  (let ((end-ptr (mr-queue-end-ptr mr-queue)))
    (set-car! end-ptr elt)
    (set-cdr! end-ptr (make-mr-queue-end))
    (set-mr-queue-end-ptr! (cdr end-ptr))))

(define (mr-queue-garbage-collect mr-queue)
  (let ((min-reader-index 
         (apply min
                (map (lambda (reader-info)
                       (reader-info-index reader-info))
                     (mr-queue-reader-infos mr-queue)))))
    (cond ((= (- min-reader-index 1) (mr-queue-deleted-count mr-queue))
           (set-mr-queue-deleted-count! min-reader-index)
           (set-mr-queue-element-list! (cdr (mr-queue-element-list mr-queue))))
          ((= min-reader-index (mr-queue-deleted-count mr-queue))
           'continue)
          (else
           (error "You screwed up! Run, you fools.")))))
  

(define (mr-queue-get-reader mr-queue)
  (let ((reader-info (make-reader-info (mr-queue-element-list mr-queue)
                                       (mr-queue-deleted-count mr-queue))))
    (set! reader-infos (cons reader-info reader-infos))
    (lambda ()
      (let ((ptr (reader-info-ptr reader-info))
            ((index (reader-info-index reader-info))))
        (let ((val (car ptr)))
          (if (not (mr-queue-end-val? val))
              (begin
                (set-reader-info-ptr! (cdr ptr))
                (set-reader-index! (+ 1 index))
                (mr-queue-garbage-collect mr-queue)))
          val)))))