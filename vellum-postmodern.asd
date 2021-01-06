(cl:in-package #:cl-user)


(asdf:defsystem vellum-postmodern
  :name "vellum-postmodern"
  :version "0.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on ( :iterate
                :serapeum
                :vellum
                :alexandria
                :cl-postgres
                :s-sql
                :documentation-utils-extensions
                :postmodern)
  :serial T
  :pathname "src"
  :components ((:file "package")
               (:file "code")))
