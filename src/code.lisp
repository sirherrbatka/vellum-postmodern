(cl:in-package #:vellum-postmodern)


(defclass postgres-query (vellum.header:frame-range-mixin
                          cl-ds:traversable)
  ((%query :initarg :query
           :reader read-query)
   (%parameters :initarg :parameters
                :reader read-parameters)
   (%prepared :initarg :prepared
              :reader read-prepared)))


(defun postgres-query (query header &key (parameters nil parameters-bound-p) (prepared parameters-bound-p))
  (when (and (not (null parameters)) (not prepared))
    (error "Parameters are only applicable to prepared queries."))
  (make 'postgres-query
        :query query
        :header header
        :parameters parameters
        :prepared prepared))


(defmethod cl-ds:traverse ((object postgres-query) function)
  (bind ((header (vellum.header:read-header object))
         (column-count (vellum.header:column-count header))
         ((:slots %query) object)
         (query (etypecase %query
                  (list (s-sql:sql-compile %query))
                  (string %query)
                  (symbol (symbol-name %query))))
         (prepared (read-prepared object))
         (parameters (read-parameters object))
         (row-reader
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
    (declare (type fixnum column-count))
    (vellum.header:with-header (header)
      (if prepared
          (cl-postgres:exec-prepared postmodern:*database* query parameters row-reader)
          (cl-postgres:exec-query postmodern:*database* query row-reader)))
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
        :prepared (read-prepared object)
        :header (vellum.header:read-header object)))


(defmethod vellum:copy-from ((format (eql ':postmodern))
                             input
                             &rest options
                             &key
                               (columns '())
                               (header (apply #'vellum.header:make-header columns))
                               (parameters nil parameters-bound-p)
                               (prepared parameters-bound-p))
  (apply #'vellum:to-table
         (postgres-query input header :parameters parameters :prepared prepared)
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
