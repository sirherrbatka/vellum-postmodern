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
             (tagbody main
                (for value = (cl-postgres:next-field (elt fields i)))
                check
                (restart-case (vellum.header:check-predicate header i value)
                  (skip-row ()
                    :report "skip this row."
                    (leave))
                  (set-to-null ()
                    :report "Set the row position to :null."
                    (setf value :null)
                    (go check))
                  (provide-new-value (v)
                    :report "Enter the new value."
                    :interactive vellum.header:read-new-value
                    (setf value v)
                    (go check))))
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
        :query (read-query object)
        :header (vellum.header:read-header object)))


(defmethod vellum:copy-from ((format (eql ':postmodern))
                             input
                             &rest options
                             &key
                               (header-class 'vellum.header:standard-header)
                               (columns '())
                               (header (apply #'vellum.header:make-header
                                              header-class columns)))
  (apply #'vellum:to-table
         (postgres-query input header)
         options))
