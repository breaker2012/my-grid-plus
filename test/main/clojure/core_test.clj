(ns core-test
  (:require [clojure.reflect :as cr]
            [clojure.test :refer :all]
            [core :refer :all]))

;(deftest a-test
;  (testing "FIXME, I fail."
;    (is (= 2 1))))

; 平方
(defn square [x] (* x x))

; 计算 tree 的平方
(defn square-tree [[f & rs]]
    (cond (instance? Number f) (concat [(square f)] (square-tree rs))
          (instance? clojure.lang.PersistentVector f) (concat [(square-tree f)] (square-tree rs))))

(def m [1 [2 [3 4] 5] [6 7] 8])
;(println (square-tree m))

(defn square-tree-second [tree]
    (cond (number? tree) (square tree)
          (empty? tree) nil
          (empty? (rest tree)) (square-tree-second (first tree))
          (list? (square-tree-second (rest tree))) (cons (square-tree-second (first tree)) (square-tree-second (rest tree)))
          :else (cons (square-tree-second (first tree)) (list (square-tree-second (rest tree))))))
;(println (square-tree-second (list 1 (list 2 (list 3 4) 5) (list 6 7))))

(defrecord Person [fname lname address])
(def stu (Person. "吴" "大富" "成都"))
(println (:lname stu))

; 求所有的子集合
(defn subsets [[f & rs]]
    (if (nil? f)
        [nil]
        (let [res (subsets rs)]
            (concat res (map #(concat [f] %) res)))))

(println (count (subsets [1 2 3 4 5])))









































