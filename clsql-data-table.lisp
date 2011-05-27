(in-package :data-table)
(cl-interpol:enable-interpol-syntax)

(defun postgres-db-type-from-lisp-type (lisp-type)
  "Given a postgres type and a modifier, return the clsql type"
  (cond ((subtypep lisp-type 'float) "double precision")
        ((subtypep lisp-type 'integer) "int8")
        ((subtypep lisp-type 'string) "text")
        ((or (subtypep lisp-type 'clsql-sys:wall-time)
             (subtypep lisp-type 'clsql-sys:date))
         "timestamp with time zone")
        (T (error "Couldnt map type"))))

(defun sql-escaped-column-names (data-table
                                 &key
                                 (transform #'english->postgres))
  (iter
    (for c in (column-names data-table))
    (unless (stringp c) (setf c (princ-to-string c)))
    (when transform (setf c (funcall transform c)))
    (collect c)))

(defun clean-name-for-db (name)
  (cl-ppcre:regex-replace-all
   #?r"(_|\(|\)|,|\.|\+|-|\?|\||\s)+"  (princ-to-string name) " "))

(defun english->mssql (name)
  (symbol-munger:english->studly-case
   (clean-name-for-db name)))

(defun english->postgres (name)
  (symbol-munger:english->underscores
   (string-downcase
    (clean-name-for-db name))))

(defun sql-escape-column-names!
    (dt &key (transform #'english->postgres))
  (setf (column-names dt)
        (sql-escaped-column-names
         dt :transform transform)))

(defun next-highest-power-of-two (l)
  (expt 2 (+ 1 (truncate (log l 2)))))

(defun mssql-db-type-from-lisp-types (data-table)
  (iter
    (for i from 0)
    (for lisp-type in (column-types data-table))
    (collect
        (cond ((subtypep lisp-type 'float) "decimal (19,9)")
              ((subtypep lisp-type 'integer)
               (iter
                 (with thresh = (expt 2 15))
                 (for int in (data-table-value data-table :col-idx i))
                 (when int
                   (minimizing int into min)
                   (maximizing int into max))
                 (finally
                  (return
                    (if (<= (- thresh) (or min 0) (or max 0) thresh)
                        "int"
                        "bigint"
                        )))))
              ((subtypep lisp-type 'string)
               (let ((next-size
                       (next-highest-power-of-two
                        (iter (for s in (data-table-value data-table :col-idx i))
                          (maximizing (length s))))))
                 (cond
                   ((< 8000 next-size) "text")
                   ((< next-size 128) #?"varchar(128)")
                   (t #?"varchar(${next-size})"))))
              ((or (subtypep lisp-type 'clsql-sys:wall-time)
                   (subtypep lisp-type 'clsql-sys:date))
               "datetime")
              (T (error "Couldnt map type"))))))

(defun ensure-mssql-table-for-data-table (data-table table-name
                                          &key (should-have-serial-id "Id")
                                          excluded-columns
                                          (database clsql-sys:*default-database*))
  (let* ((dt data-table)
         (sql-types (mssql-db-type-from-lisp-types data-table))
         (cmd
           (collectors:with-string-builder (body :delimiter #?",\n  ")
             (when should-have-serial-id
               (body #?"${should-have-serial-id} int IDENTITY (1,1) PRIMARY KEY"))
             (iter (for type in sql-types)
               (for c in (column-names dt))
               (unless (member c excluded-columns :test #'string-equal)
                 (body (format nil "~a ~a" c type))))
             #?"CREATE TABLE dbo.${table-name} ( ${(body)} );")))
    (if database
        (unless (clsql-sys:table-exists-p table-name)
          (clsql-sys:execute-command cmd))
        (warn "No Database, skipping creating table"))
    cmd))


(defun ensure-postgres-table-for-data-table (data-table table-name
                                             &key (should-have-serial-id "id") (schema "public")
                                             excluded-columns)
  (let* ((dt data-table))
    (sql-escape-column-names! dt)
    (unless (clsql-sys:table-exists-p table-name)
      (let ((cmd (collectors:with-string-builder (body :delimiter #?",\n  ")
                   (when should-have-serial-id
                     (body #?"\"${should-have-serial-id}\" serial PRIMARY KEY"))
                   (iter (for type in (column-types dt))
                     (for c in (column-names dt))
                     (unless (member c excluded-columns :test #'string-equal)
                       (body (format nil "~a ~a" c (postgres-db-type-from-lisp-type type)))))
                   #?"CREATE TABLE ${schema}.${table-name} ( ${(body)} );")))
        (clsql-sys:execute-command cmd)))))

(defun import-data-table-to-postgres (data-table table-name
                                      &key (schema "public") excluded-columns
                                      (column-name-transform #'english->postgres))
  (let ((cols (sql-escaped-column-names
               data-table :transform column-name-transform))
        (cl-interpol:*list-delimiter* ",")
        (*print-pretty* nil))
    (iter (for row in (rows data-table))
      (for data = (iter (for d in row)
                    (for c in (column-names data-table))
                    (unless (member c excluded-columns :test #'string-equal)
                      (collect (format-value-for-postgres d)))))
      (clsql-sys:execute-command
       #?"INSERT INTO ${schema}.${table-name} (@{ cols }) VALUES ( @{data} )"))))

(defun import-data-table-to-mssql (data-table table-name &key excluded-columns)
  (let ((cols (sql-escaped-column-names data-table :transform #'english->mssql))
        (cl-interpol:*list-delimiter* ",")
        (*print-pretty* nil))
    (iter (for row in (rows data-table))
      (for data = (iter (for d in row)
                    (for c in (column-names data-table))
                    (unless (member c excluded-columns :test #'string-equal)
                      (collect (format-value-for-database d)))))
      (clsql-sys:execute-command
       #?"INSERT INTO dbo.${table-name} (@{ cols }) VALUES ( @{data} )"))))
