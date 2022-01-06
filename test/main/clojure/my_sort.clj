(ns my-sort
  (:require [clojure.test :refer :all]))

(defn merge_lst
  ([lst1 lst2] (merge_lst lst1 lst2 []))
  ([lst1 lst2 lst]
   (let [f (first lst1) f1 (first lst2) r (rest lst1) r1 (rest lst2)]
     (cond (and (some? f) (some? f1)) (if (<= f f1) (recur r lst2 (conj lst f))
                                                    (recur lst1 r1 (conj lst f1)))
           (and (nil? f) (some? f1)) (concat lst lst2)
           (and (nil? f1) (some? f)) (concat lst lst1)
           :else
           lst
           ))))

(defn merge_all_lst [[f & r]]
  (if (and (some? f) (some? (first r)))
    (recur (cons (merge_lst f (first r)) (rest r)))
    f))

(defn merge_all_lst_2
  ([lst] (merge_all_lst_2 lst []))
  ([[f & r] lst]
   (cond (and (some? f) (some? (first r))) (let [m (conj lst (merge_lst f (first r)))]
                                             (recur (rest r) m))
         (and (some? f) (nil? (first r))) (cond (> (count lst) 1) (recur (conj lst f) [])
                                                (= (count lst) 1) (merge_lst f (first lst))
                                                :else
                                                f)
         (nil? f) (cond (> (count lst) 1) (recur lst [])
                        (= (count lst) 1) (first lst))
         )))

(defn get_lst [my_count my_num]
  (loop [n my_count lst []]
    (if (> n 0)
      (recur (dec n) (conj lst (rand-int my_num)))
      lst)))

(defn gen-vector [n]
  (take n (repeatedly #(rand-int n))))

(defn gen-table [row_n n]
  (loop [row_lst (map #(+ % 1) (get_lst row_n n)) table []]
    (if (some? (first row_lst))
      (recur (rest row_lst) (conj table (sort (gen-vector (first row_lst)))))
      table)))

(defn gen-table [row_n n]
  (loop [row_lst (map #(+ % n) (gen-vector row_n)) table []]
    (if (some? (first row_lst))
      (recur (rest row_lst) (conj table (sort (gen-vector (first row_lst)))))
      table)))


(defn my-test [n m]
  (let [table (gen-table n m)]
    (println (time (merge_all_lst_2 table)))
    (println (time (merge_all_lst table)))))