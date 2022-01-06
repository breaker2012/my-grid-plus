(ns scip.quick-sort
  (:require [clojure.test :refer :all]))

(defn quick-sort-iter [seq]
  (letfn [(iter [rest done]
            (let [fir (first rest)
                  ls (filter #(< % fir) rest)
                  self (filter #(= % fir) rest)
                  rs (filter #(> % fir) rest)]
              (cond (empty? seq) nil
                    (empty? rest) done
                    (empty? ls) (iter rs (concat done self))
                    (empty? rs) (iter ls (concat done self))
                    :else (iter (concat ls self rs) done)
                    )))]
    (iter seq '()))
  )

(println (quick-sort-iter '(4 21 2 23 5 7 89 3)))

(defn my_iter [[f & r] done]
    (let [ls (filter #(< % f) r) self (filter #(= % f) r) rs (filter #(> % f) r)]
        (cond (empty? r) done
              (empty? ls) (cond (empty? rs) (concat done self)
                                :else
                                (recur rs (concat done self)))
              :else
              (recur (concat ls self rs) done))))

(defn my_iter [[f & r] done]
    (if (some? f)
        (let [ls (filter #(< % f) r) self (filter #(= % f) r) rs (filter #(> % f) r)]
            (cond (and (empty? ls) (empty? rs)) (concat done self)
                  (and (empty? ls) (some? rs)) (recur rs (concat done self))
                  (and (some? ls) (empty? rs)) (recur ls (concat self done))
                  :else
                  (recur (concat ls self rs) done)
                  )) done))

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

(defn quick-sort-iter-1 [seq]
    (letfn [(iter [rest done]
                (let [fir (first rest)
                      ls (filter #(< % fir) rest)
                      self (filter #(= % fir) rest)
                      rs (filter #(> % fir) rest)]
                    (cond (empty? seq) nil
                          (empty? rest) done
                          (empty? ls) (if (empty? rs) (concat done self)
                                                      (iter rs (concat done self)))
                          :else (iter (concat ls self rs) done)
                          )
                    )
                )]
        (iter seq '()))
    )

(println (quick-sort-iter-1 '(4 21 2 23 5 7 89 3)))

(defn quick-sort-iter-2 [seq]
    (letfn [(iter [rest done]
                (let [fir (first rest)
                      ls (filter #(< % fir) rest)
                      self (filter #(= % fir) rest)
                      rs (filter #(> % fir) rest)]
                    (cond (empty? seq) nil
                          (empty? rest) done
                          (empty? ls) (if (empty? rs) (concat done self)
                                                      (recur rs (concat done self)))
                          :else (recur (concat ls self rs) done)
                          )
                    )
                )]
        (iter seq '()))
    )

(println (quick-sort-iter-2 (reverse (range 1 9999))))
; 参考