(ns scip.my-quick-sort
  (:require [clojure.test :refer :all]))

; 尾递归优化
(defn my_add [n]
  (if (> n 0)
    (+ n (my_add (dec n))) 0))

; 执行 (my_add 9000 0) 会报错

(defn my_add_1 [n my_result]
  (if (= n 0) my_result
              (recur (dec n) (+ n my_result))))
; 执行 (my_add_1 9000 0) 就是正确的

(defn my_quick_sort
  ([lst] (my_quick_sort lst []))
  ([lst lst_result]
   (let [f (first lst)]
     (if (some? f)
       (let [ls (filter #(< % f) lst) self (filter #(= % f) lst) rs (filter #(> % f) lst)]
         (cond (and (empty? ls) (empty? rs)) (concat lst_result self)
               (and (empty? ls) (some? rs)) (recur rs (concat lst_result self))
               :else
               (recur (concat ls self rs) lst_result)
               )) lst_result))))

;(println (my_quick_sort [23 4 54 87 12 1 7 21 32]))
(println (my_quick_sort (reverse (range 1 9000))))