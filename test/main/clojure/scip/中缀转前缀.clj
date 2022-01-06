(ns scip.中缀转前缀
  (:require [clojure.test :refer :all]))

(defn variable? [x]
  (symbol? x))
(defn rest-obj [x]
  (first (rest x)))
(defn mid-to-forward [exp]
  (cond (or (number? exp) (variable? exp)) exp
        (empty? (rest exp)) (mid-to-forward (first exp))
        (= (rest-obj exp) '+) (list (rest-obj exp) (mid-to-forward (first exp)) (mid-to-forward (rest (rest exp))))
        (= (rest-obj exp) '*) (if (not (empty? (rest (rest (rest exp)))))
                                (if (= '+ (first (rest (rest (rest exp))))) (list '+ (list (rest-obj exp) (mid-to-forward (first exp)) (mid-to-forward (first (rest (rest exp))))) (mid-to-forward (rest (rest (rest (rest exp))))))
                                                                            (list (rest-obj exp) (mid-to-forward (first exp)) (mid-to-forward (rest (rest exp))))
                                                                            )
                                (list (rest-obj exp) (mid-to-forward (first exp)) (mid-to-forward (rest (rest exp)))))
        )
  )

(println (mid-to-forward '((3 + 6) + 4 * x + 7 + (8 + 9) * (x + 11))))