;; -*- lisp -*-

(eval-when (:compile-toplevel :load-toplevel :execute)
  (unless (find-package :data-table.system)
    (defpackage :data-table.system
	(:use :common-lisp :asdf))))

(in-package data-table.system)

(defsystem :data-table-clsql
  :description "A library providing a data-table class, and useful functionality around this"
  :licence "BSD"
  :version "0.1"
  :components ((:file "clsql-data-table"))
  :depends-on (:iterate :clsql :clsql-helper :collectors :data-table))

(defmethod asdf:perform ((o asdf:test-op) (c (eql (find-system :data-table-clsql))))
  (asdf:test-system :data-table))

;; Copyright (c) 2011 Russ Tyndall , Acceleration.net http://www.acceleration.net

;; Permission is hereby granted, free of charge, to any person obtaining
;; a copy of this software and associated documentation files (the
;; "Software"), to deal in the Software without restriction, including
;; without limitation the rights to use, copy, modify, merge, publish,
;; distribute, sublicense, and/or sell copies of the Software, and to
;; permit persons to whom the Software is furnished to do so, subject to
;; the following conditions:

;; The above copyright notice and this permission notice shall be
;; included in all copies or substantial portions of the Software.

;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
;; EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
;; MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
;; NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
;; LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
;; OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
;; WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.