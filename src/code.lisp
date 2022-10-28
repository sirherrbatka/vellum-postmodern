(cl:in-package #:vellum-postmodern)


(defclass postgres-query (vellum.header:frame-range-mixin
                          cl-ds:traversable)
  ((%query :initarg :query
           :reader read-query)))


(defclass postgres-simple-query (postgres-query)
  ())


(defclass postgres-prepared-query (postgres-query)
  ((%base-parameters :initarg :base-parameters
                     :reader read-base-parameters)
   (%parameters :initarg :parameters
                :reader read-parameters)
   (%query-name :initarg :query-name
                :reader read-query-name)))


(defun postgres-simple-query (query header)
  (make 'postgres-simple-query :query query :header header))


(defun postgres-prepared-query (query columns base-parameters query-name)
  (make 'postgres-prepared-query :query query
				 :header (apply #'vellum.header:make-header columns)
				 :parameters nil
				 :base-parameters base-parameters
				 :query-name query-name))


(defun to-sql-string (query)
  (etypecase query
    (list (s-sql:sql-compile query))
    (string query)))


(defun prepare-query (query columns parameters &optional (query-name ""))
  (let ((query (to-sql-string query)))
    (cl-postgres:prepare-query postmodern:*database*
			       query-name
			       query
			       parameters)
    (when (postmodern:connected-p postmodern:*database*)
      (postgres-prepared-query query columns parameters query-name))))


(defgeneric postgres-exec-query (object &optional row-reader))


(defmethod postgres-exec-query ((object postgres-simple-query)
                                &optional row-reader)
  (bind (((:slots %query) object)
         (query (to-sql-string %query)))
    (cl-postgres:exec-query postmodern:*database* query row-reader)))


(defmethod postgres-exec-query ((object postgres-prepared-query)
                                &optional row-reader)
  (bind (((:slots %query-name %parameters) object))
    (cl-postgres:exec-prepared postmodern:*database*
			       %query-name
			       %parameters
			       row-reader)))


(defmethod cl-ds:traverse ((object postgres-query) function)
  (bind ((header (vellum.header:read-header object))
         (column-count (vellum.header:column-count header)))
    (declare (type fixnum column-count))
    (vellum.header:with-header (header)
      (postgres-exec-query object
       (cl-postgres:row-reader (fields)
         (declare (type simple-vector fields))
         (unless (= column-count (length fields))
           (error "Number of columns in the header does not match number of selected fields in query."))
         (iterate
           (while (cl-postgres:next-row))
           (iterate
             (declare (type fixnum i)
                      (type simple-vector row))
             (with row = (make-array column-count))
             (for i from 0 below column-count)
             (for value = (cl-postgres:next-field (svref fields i)))
             (setf (svref row i) value)
             (finally
              (vellum.header:set-row row)
              (funcall function row)))))))
    object))


(defmethod cl-ds:across ((object postgres-query) function)
  (cl-ds:traverse object function))


(defmethod cl-ds.alg.meta:across-aggregate ((object postgres-query) function)
  (cl-ds:traverse object function))


(defmethod cl-ds:reset! ((object postgres-query))
  object)


(defmethod cl-ds:clone ((object postgres-query))
  (make 'postgres-query
        :query (read-query object)
        :header (vellum.header:read-header object)))

(defun postgres-prepared-query? (object)
  (eq (type-of object) 'postgres-prepared-query))


(defmethod vellum:copy-from ((format (eql ':postmodern))
                             input
                             &rest options
                             &key
                               (columns '())
                               (header (apply #'vellum.header:make-header
                                              columns))
                               (parameters '()))
  (apply #'vellum:to-table
	 (if (postgres-prepared-query? input)
	     (prog1 input
	       (setf (slot-value input '%parameters) parameters
		     (slot-value input 'vellum.header::%header) header))
	     (postgres-simple-query input header))
	 options))


(defmethod vellum:copy-to ((format (eql ':postmodern))
                           table-name
                           input
                           &rest options &key (batch-size 50))
  (declare (ignore options))
  (let ((column-count (vellum:column-count input)))
    (vellum:pipeline (input)
      (cl-ds.alg:on-each (vellum:bind-row ()
                           (iterate
                             (for i from 0 below column-count)
                             (collecting (vellum:rr i)))))
      (cl-ds.alg:in-batches batch-size)
      (cl-ds.alg:to-list
       :after (lambda (batch)
                (postmodern:execute (:insert-rows-into table-name
                                     :values batch))
                nil))))
  input)
