(cl:in-package #:vellum-postmodern)


(defclass postgres-query (vellum.header:frame-range-mixin
                          cl-ds:traversable)
  ((%query :initarg :query
           :reader read-query)))


(defun postgres-query (query header)
  (make 'postgres-query :query query :header header))


(defmethod cl-ds:traverse ((object postgres-query) function)
  (bind ((header (vellum.header:read-header object))
         (column-count (vellum.header:column-count header))
         ((:slots %query) object)
         (query (etypecase %query
                  (list (s-sql:sql-compile %query))
                  (string %query))))
    (declare (type fixnum column-count))
    (vellum.header:with-header (header)
      (cl-postgres:exec-query
       postmodern:*database* query
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


(defmethod vellum:copy-from ((format (eql ':postmodern))
                             input
                             &rest options
                             &key
                               (columns '())
                               (header (apply #'vellum.header:make-header
                                              columns)))
  (apply #'vellum:to-table
         (postgres-query input header)
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
