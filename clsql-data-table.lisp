(in-package :data-table)
(cl-interpol:enable-interpol-syntax)

(defun exec (command)
  (if clsql-sys:*default-database*
    (clsql-sys:execute-command command)
    (warn "No Database (bind clsql-sys:*default-database*) : cant exec ~A" command)))

(defun has-table? (table)
  (clsql-sys:table-exists-p table))


(defmethod get-data-table ( query &key auto-type )
  "When Auto-type is true it will work to ensure everything is in a reasonable data-type and document what type that is"
  (multiple-value-bind (rows colnames) (clsql:query query :flatp T)
    (let ((dt (make-instance 'data-table :rows rows :column-names colnames )))
      (when auto-type
	(coerce-data-table-of-strings-to-types dt))
      dt)))

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

(defmethod is-clsql-date-type? (type)
  (or (subtypep type 'clsql-sys:wall-time)
      (subtypep type 'clsql-sys:date)))

(defmethod %to-clsql-date (val)
  (clsql-helper:convert-to-clsql-datetime val))

(defun next-highest-power-of-two (l)
  (expt 2 (+ 1 (truncate (log l 2)))))

(defun ensure-mssql-table-for-data-table (data-table table-name
                                          &key (should-have-serial-id "Id")
                                          excluded-columns)
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
    (unless (has-table? table-name)
      (exec cmd))
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
        (exec cmd)))))

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
                      (collect (clsql-helper:format-value-for-database d)))))
      (exec
       #?"INSERT INTO ${schema}.${table-name} (@{ cols }) VALUES ( @{data} )"))))

(defun import-data-table-to-mssql (data-table table-name &key excluded-columns)
  (let ((cols (sql-escaped-column-names data-table :transform #'english->mssql))
        (cl-interpol:*list-delimiter* ",")
        (*print-pretty* nil))
    (iter (for row in (rows data-table))
      (for data = (iter (for d in row)
                    (for c in (column-names data-table))
                    (unless (member c excluded-columns :test #'string-equal)
                      (collect (clsql-helper:format-value-for-database d)))))
      (exec
       #?"INSERT INTO dbo.${table-name} (@{ cols }) VALUES ( @{data} )"))))
