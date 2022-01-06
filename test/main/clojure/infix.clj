(ns infix
  (:require [clojure.test :refer :all]))

(declare infix-fn)

(defn my-args [lst]
  (if (list? lst) (infix-fn lst) lst))

(defn infix-fn [[a op b]]
  (list op (my-args a) (my-args b)))

(defmacro infix [form]
  (infix-fn form))