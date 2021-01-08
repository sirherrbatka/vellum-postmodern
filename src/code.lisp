(cl:in-package #:vellum-postmodern)


(defclass postgres-query (vellum.header:frame-range-mixin
                          cl-ds:traversable)
  ((%query :initarg :query
           :reader read-query)))


(defmethod cl-ds:traverse ((object postgres-query) function)
  (bind ((header (vellum.header:read-header object))
         (column-count (vellum.header:column-count header))
         ((:slots %query) object)
         (query (etypecase %query
                  (list (s-sql:sql-compile %query))
                  (string %query))))
    (vellum.header:with-header (header)
      (cl-postgres:exec-query
       postmodern:*database* query
       (cl-postgres:row-reader (fields)
         (unless (= column-count (length fields))
           (error "Number of columns in the header does not match number of selected fields in query."))
         (iterate
           (while (cl-postgres:next-row))
           (iterate
             (with row = (make-array column-count))
             (for i from 0 below column-count)
             (for value = (cl-postgres:next-field (elt fields i)))
             (vellum.header:check-predicate header i value)
             (setf (aref row i) value)
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
        :query (read-query object)))


(defmethod vellum:copy-from ((format (eql ':postmodern))
                             input
                             &rest options
                             &key header)
  (apply #'vellum:to-table
         (make 'postgres-query :query input :header header)
         options))
