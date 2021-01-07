(cl:in-package #:vellum-postmodern)


(defclass postgres-query (cl-ds:traversable)
  ((%query :initarg :query
           :type list
           :reader read-query)))


(defmethod cl-ds:traverse ((object postgres-query) function)
  (bind ((header (vellum.header:header))
         (column-count (vellum.header:column-count header))
         (row (make-array column-count :initial-element :null))
         ((:slots %query) object)
         (query (etypecase %query
                  (list (s-sql:sql-compile %query))
                  (string %query))))
    (vellum.header:set-row row)
    (cl-postgres:exec-query
     postmodern:*database* query
     (cl-postgres:row-reader (fields)
       (unless (= column-count (length fields))
         (error "Number of columns in the header does not match number of selected fields in query."))
       (iterate
         (while (cl-postgres:next-row))
         (iterate
           (for i from 0 below column-count)
           (setf (aref row i) (cl-postgres:next-field (elt fields i))))
         (funcall function row))))
    object))


(defmethod cl-ds:across ((object postgres-query) function)
  (cl-ds:traverse object function))


(defmethod cl-ds:reset! ((object postgres-query))
  object)


(defmethod cl-ds:clone ((object postgres-query))
  (make 'postgres-query
        :query (read-query object)))


(defmethod vellum:copy-from ((format (eql ':postmodern))
                            (input list)
                            &rest options)
  (apply #'vellum:to-table
         (make 'postgres-query :query input)
         options))


(defmethod vellum:to-table ((object postgres-query)
                           &key
                             (key #'identity)
                             (class 'vellum.table:standard-table)
                             (header-class 'vellum.header:standard-header)
                             (columns '())
                             (body nil)
                             (header (apply #'vellum.header:make-header
                                            header-class columns)))
  (let* ((function (if (null body)
                       (constantly nil)
                       (vellum:bind-row-closure body)))
         (table (vellum:make-table :class class :header header))
         (transformation (vellum.table:transformation table nil
                                                      :in-place t))
         (fn (lambda (content)
               (vellum:transform-row
                transformation
                (lambda ()
                  (iterate
                    (for c in-vector content)
                    (for i from 0)
                    (setf (vellum:rr i) (funcall key c))
                    (finally (funcall function))))))))
    (cl-ds:across object fn)
    (vellum:transformation-result transformation)))
