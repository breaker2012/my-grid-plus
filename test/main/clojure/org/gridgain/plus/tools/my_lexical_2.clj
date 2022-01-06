(ns org.gridgain.plus.tools.my-lexical-2
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [clojure.string :as str]))


(def line "select \n                        /* 描述 */\n                        c.description as description,\n                        (select -- 员工名称\n emp_name from staff_info where empno=a.empno) as emp_name,\n                                       -- 国家代码\n                                       a.region_code\n                                  from lcs_dept_hiberarchy_trace b,\n                                       agent_info a,\n                                       agent_rank_tbl c,\n                                       (select emp_name from staff_info where empno=a.empno) as d\n                                 where a.empno = c_empno\n                                   and exists (select emp_name from staff_info where empno=a.empno)\n                                   GROUP BY b.authorid  HAVING my_count = 2 \n                                   order by c.region_grade desc, a.age asc  LIMIT 0, 2;")

; 添加删除注释的语句
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
           (and (= f \<) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["<"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))

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

