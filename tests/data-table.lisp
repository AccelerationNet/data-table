(defpackage :data-table-test
  (:use :cl :cl-user :data-table :lisp-unit :iter))

(in-package :data-table-test)

(defun test-data-table ()
  (make-instance
   'data-table
   :column-names '("first name" "last name" "job title" "number of hours" "id")
   :rows '(("Russ" "Tyndall" "Software Developer" "26.2" "1")
           ("Adam" "Smith" "Economist" "37.5" "2")
           ("John" "Doe" "Anonymous Human" "42.1" "3")
           ("Chuck" "Darwin" "Natural Philosipher" "17.68" "4")
           ("Bill" "Shakespear" "Bard" "12.2" "5")
           ("James" "Kirk" "Starship Captain" "13.1" "6"))))

(define-test data-table-types
  (let ((dt (test-data-table)))
    (assert-false (column-types dt))
    (coerce-data-table-of-strings-to-types dt)
    (assert-equal
        '(string string string double-float integer)
        (column-types dt))
    (iter
      (for i from 0 below (length (column-names dt)))
      (for type in (column-types dt))
      (iter
        (for j from 0 below (length (rows dt)))
        (assert-true
            (typep (data-table-value dt :row-idx j :col-idx i)
                   type))))))

(define-test data-table-value-manip
  (let ((dt (test-data-table)))
    (assert-eql 5 (length (column-names dt)) dt)
    (add-column dt "color" nil 'string 1)
    (assert-eql 6 (length (column-names dt)) dt)
    (assert-eql nil (data-table-value dt :col-idx 1 :row-idx 3))
    (setf (data-table-value dt :col-idx 1) (list "blue" "green" "yellow" "orange"))
    (assert-equal (list "blue" "green" "yellow" "orange" nil nil)
        (data-table-value dt :col-idx 1) dt)
    (assert-equal "blue"
        (data-table-value dt :col-idx 1 :row-idx 0) dt)
    ))

(define-test data-table-value-manip2
  (let ((dt (test-data-table)))
    (setf (data-table-value dt :col-idx 3 :row-idx 1) 3)
    (assert-equal 3 (data-table-value dt :col-idx 3 :row-idx 1))
    (assert-equal 5 (length (data-table-value dt :row-idx 1)))
    ))

(define-test data-table-value-overlaying
  (let ((dt-targ (make-instance 'data-table))
        (dt-copy1 (make-instance 'data-table
                                 :rows '(("a" "b")
                                         ("c" "d"))))
        (dt-copy2 (make-instance 'data-table
                                 :rows '(("1" "2")
                                         ("3" "4")))))


    (overlay-region dt-copy1 dt-targ :row-idx 1 :col-idx 1)
    (assert-true 3 (number-of-columns dt-targ))
    (assert-true 3 (number-of-rows dt-targ))

    (overlay-region dt-copy2 dt-targ :row-idx 3 :col-idx 3)
    (assert-true 5 (number-of-columns dt-targ))
    (assert-true 5 (number-of-rows dt-targ))
    (overlay-region dt-copy1 dt-targ :row-idx 2 :col-idx 2)
    (overlay-region dt-copy2 dt-targ :row-idx 3 :col-idx 0)
    (assert-true 5 (number-of-columns dt-targ))
    (assert-true 5 (number-of-rows dt-targ))

    ;; (fill-in-missing-cells dt-targ)
;;  make sure we have a square data-table again
;;    0: (NIL NIL NIL NIL NIL)
;;    1: (NIL "a" "b" NIL NIL)
;;    2: (NIL "c" "a" "b" NIL)
;;    3: ("1" "2" "c" "d" "2")
;;    4: ("3" "4" NIL "3" "4")

    (iter (for row in (rows dt-targ))
      (assert-eql 5 (length row)))
    ;; Convert our random strings into types we can see
    (coerce-data-table-of-strings-to-types dt-targ)
    (assert-equal '(integer string string string integer)
        (column-types dt-targ))
    (assert-equal '(1 "2" "c" "d" 2)
        (data-table-value dt-targ :row-idx 3))
    ))

(define-test data-table-subtables
  (let* ((dt (make-instance
              'data-table
              :column-names '(i j k x y z)
              :column-types '(integer integer integer symbol symbol symbol)
              :rows '((1 2 3 a b c)
                      (3 4 5 d e f)
                      (6 7 8 g h i))))
         (man-sub
           (make-instance
            'data-table
            :rows '((1 2 3)
                    (3 4 5)
                    (6 7 8))))
         (dts1 (make-sub-table dt :lci 0 :uci 3))
         (dts2 (make-sub-table dt :lci 3 :uci 6)))
    (assert-equal 3 (number-of-columns dts1))
    (assert-equal 3 (number-of-columns dts2))
    (assert-equal '(i j k) (column-names dts1))
    (assert-equal '(x y z) (column-names dts2))
    (assert-equal 4 (data-table-value dts1 :col-idx 1 :row-idx 1))
    (assert-equal 'e (data-table-value dts2 :col-idx 1 :row-idx 1))
    (assert-true (data-table-data-compare dts1 man-sub) dts1 man-sub)))

(define-test data-table-alist
  (let* ((als '(((:a . 1) (:b . 2) (:c . 3))
                ((:b . 4) (:a . 5) (:c . 6) (:d :not-in-data-table))
                ((:c . 9) (:a . 8) (:b . 7))
                ((:b . 10) (:c . 11) (:a . 12)))
              )
         (dt (alists-to-data-table als))
         (als (data-table-to-alists dt)))
    (assert-equal
        '(:a :b :c)
        (column-names dt))
    (assert-equal '(2 4 7 10)
        (data-table-value dt :col-name :b))
    (assert-equal 6
        (data-table-value dt :col-name :c :row-idx 1))
    (assert-equal
        '(((:a . 1) (:b . 2) (:c . 3))
          ((:a . 5) (:b . 4) (:c . 6))
          ((:a . 8) (:b . 7)(:c . 9) )
          ((:a . 12) (:b . 10) (:c . 11)))
        als)))

(define-test data-table-plist
  (let* ((pls '(( :a  1 :b  2 :c  3)
                ( :b  4 :a  5 :c  6 :d :not-in-data-table)
                ( :c  9 :a  8 :b  7)
                ( :b  10 :c  11 :a  12)))
         (dt (plists-to-data-table pls))
         (pls (data-table-to-plists dt)))
    (assert-equal '(:a :b :c)
        (column-names dt))
    (assert-equal 4 (number-of-rows dt))
    (assert-equal 3 (number-of-columns dt))
    (assert-equal '(2 4 7 10)
        (data-table-value dt :col-name :b))
    (assert-equal 6
        (data-table-value dt :col-name :c :row-idx 1))
    (assert-equal
        '(( :a  1 :b  2 :c  3)
          ( :a  5 :b  4 :c  6)
          ( :a  8 :b  7 :c  9 )
          ( :a  12 :b  10 :c  11 ))
        pls)))

