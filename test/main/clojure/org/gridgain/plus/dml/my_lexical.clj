(ns org.gridgain.plus.dml.my-lexical
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [clojure.string :as str]))

; 1、按空格和特殊字符把字符串转换成数组
(defn to-back
  ([line] (to-back line [] [] [] []))
  ([[f & rs] stack-str stack-zhushi-1 stack-zhushi-2 lst]
   (if (some? f)
     (cond (and (= f \space) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst)] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 []))
           (and (= f \() (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "("] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["("] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \)) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ")"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [")"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))

           (and (= f \/) (= (first rs) \*) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back (rest rs) stack-str (conj stack-zhushi-1 1) stack-zhushi-2 lst)
           (and (= f \*) (= (first rs) \/) (= (count stack-str) 0) (> (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back (rest rs) stack-str (pop stack-zhushi-1) stack-zhushi-2 [])
           (and (= f \-) (= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back (rest rs) stack-str stack-zhushi-1 (conj stack-zhushi-2 1) lst)

           (and (= f \,) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ","] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [","] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \+) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "+"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["+"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \-) (not= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "-"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["-"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \*) (not= (first rs) \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "*"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["*"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "/"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["/"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "="] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["="] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [">="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [">"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["<="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \<) (some? (first rs)) (not= (first rs) \=) (not= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["<"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
           (and (= f \<) (some? (first rs)) (= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<>"] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["<>"] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])))

           (and (= f \") (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back rs (conj stack-str [f "双"]) stack-zhushi-1 stack-zhushi-2 (conj lst f))
           (and (= f \") (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                              (cond (= (nth t 1) "双") (to-back rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f))
                                                                                                                    :else
                                                                                                                    (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f)))
                                                                                                              )
           (and (= f \') (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back rs (conj stack-str [f "单"]) stack-zhushi-1 stack-zhushi-2 (conj lst f))
           (and (= f \') (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                              (cond (= (nth t 1) "单") (to-back rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f))
                                                                                                                    :else
                                                                                                                    (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f)))
                                                                                                              )
           (and (= f \newline) (= (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (to-back (concat [\space] rs) stack-str stack-zhushi-1 stack-zhushi-2 lst)
           (and (= f \newline) (> (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (to-back rs stack-str stack-zhushi-1 (pop stack-zhushi-2) [])
           :else (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f))
           )
     (if (> (count lst) 0) [(str/join lst)])
     )))

(defn to-back-1
    ([line] (reverse (to-back-1 line [] [] [] [] [])))
    ([[f & rs] stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result]
     (if (some? f)
         (cond (and (= f \space) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst)] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] lst_result))
               (and (= f \() (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "("] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["("] lst_result)))
               (and (= f \)) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) ")"] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [")"] lst_result)))

               (and (= f \/) (= (first rs) \*) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (conj stack-zhushi-1 1) stack-zhushi-2 lst lst_result)
               (and (= f \*) (= (first rs) \/) (= (count stack-str) 0) (> (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (pop stack-zhushi-1) stack-zhushi-2 [] lst_result)
               (and (= f \-) (= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str stack-zhushi-1 (conj stack-zhushi-2 1) lst lst_result)

               (and (= f \,) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) ","] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [","] lst_result)))
               (and (= f \+) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "+"] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["+"] lst_result)))
               (and (= f \-) (not= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "-"] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["-"] lst_result)))
               (and (= f \*) (not= (first rs) \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "*"] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["*"] lst_result)))
               (and (= f \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "/"] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["/"] lst_result)))
               (and (= f \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "="] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["="] lst_result)))
               (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) ">="] lst_result)) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [">="] lst_result)))
               (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) ">"] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [">"] lst_result)))
               (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "<="] lst_result)) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["<="] lst_result)))
               (and (= f \<) (some? (first rs)) (not= (first rs) \=) (not= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "<"] lst_result)) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["<"] lst_result)))
               (and (= f \<) (some? (first rs)) (= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat [(str/join lst) "<>"] lst_result)) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat ["<>"] lst_result)))

               (and (= f \") (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "双"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \") (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "双") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \') (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "单"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \') (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "单") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \newline) (= (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur (concat [\space] rs) stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result)
               (and (= f \newline) (> (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur rs stack-str stack-zhushi-1 (pop stack-zhushi-2) [] lst_result)
               :else (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               )
         (if (> (count lst) 0) (concat [(str/join lst)] lst_result) lst_result)
         )))

(defn to-back-2
    ([line] (to-back-2 line [] [] [] [] []))
    ([[f & rs] stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result]
     (if (some? f)
         (cond (and (= f \space) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst)])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] lst_result))
               (and (= f \() (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "("])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["("])))
               (and (= f \)) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ")"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [")"])))

               (and (= f \/) (= (first rs) \*) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (conj stack-zhushi-1 1) stack-zhushi-2 lst lst_result)
               (and (= f \*) (= (first rs) \/) (= (count stack-str) 0) (> (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (pop stack-zhushi-1) stack-zhushi-2 [] lst_result)
               (and (= f \-) (= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str stack-zhushi-1 (conj stack-zhushi-2 1) lst lst_result)

               (and (= f \,) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ","])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [","])))
               (and (= f \+) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "+"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["+"])))
               (and (= f \-) (not= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "-"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["-"])))
               (and (= f \*) (not= (first rs) \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "*"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["*"])))
               (and (= f \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "/"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["/"])))
               (and (= f \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "="])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["="])))
               (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ">="])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [">="])))
               (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ">"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [">"])))
               (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<="])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<="])))
               (and (= f \<) (some? (first rs)) (not= (first rs) \=) (not= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<"])))
               (and (= f \<) (some? (first rs)) (= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<>"])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<>"])))

               (and (= f \") (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "双"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \") (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "双") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \') (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "单"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \') (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "单") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \newline) (= (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur (concat [\space] rs) stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result)
               (and (= f \newline) (> (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur rs stack-str stack-zhushi-1 (pop stack-zhushi-2) [] lst_result)
               :else (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               )
         (if (> (count lst) 0) (concat lst_result [(str/join lst)]) lst_result)
         )))

(def sql-line "select from (wu da  fu) is suaige")

(println (to-back-2 sql-line))
(println (type (to-back-1 sql-line)))




































