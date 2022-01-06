(ns org.gridgain.plus.tools.my-lexical-1
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [clojure.string :as str]))

; 定义 token
; 定义关键词 token
; select from where insert values update delete
(defrecord Keyword [text])

; 定义 文字 select name from user
; 这里的 name 就是文字
(defrecord Literal [text])

; 定义 文字 select name, "wufafu" from user
; 这里的 "wufafu" 就是 Literal-String
(defrecord Literal-String [text])

; 定义 文字 select name, 12345 from user
; 这里的 12345 就是 Literal-Number
(defrecord Literal-Number [text])

(defrecord Punctuation [text])

(defrecord Operator [text])

(defrecord Comparison [text])

(defrecord Whitespace [text])

; 左小括号
(defrecord Lparen [text])

; 右小括号
(defrecord Rparen [text])

; 操作 m.id + 1
(defrecord Opertation [expression])


; 1、工具方法
; 1.1、判断是 select 开头的语句
(defn is-select? [sql_line]
  (re-find #"^(?i)select\s+|^(?i)select\*" sql_line))


; 1.2、判断是 UPDATE 开头的语句
(defn is-update? [sql_line]
  (re-find #"^(?i)UPDATE\s+|^(?i)UPDATE\*" sql_line))

; 1.3、判断是 INSERT 开头的语句
(defn is-insert? [sql_line]
  (re-find #"^(?i)INSERT\s+into\s+|^(?i)INSERT\*" sql_line))

; 1.4、判断是 DELETE 开头的语句
(defn is-delete? [sql_line]
  (re-find #"^(?i)DELETE\s+from\s+|^(?i)DELETE\*" sql_line))

; 1.5、transaction 事务
(defn is-begin? [sql_line]
  (re-find #"^(?i)begin\:" sql_line))

; 1.6、create table
(defn is-create-table? [sql_line]
  (re-find #"^(?i)create\s+table\s+" sql_line))

; 1.7、alert table
(defn is-alter-table? [sql_line]
  (re-find #"^(?i)alter\s+table\s+" sql_line))

; 1.8、drop table
(defn is-drop-table? [sql_line]
  (re-find #"^(?i)drop\s+table\s+" sql_line))

; 1.9、create index
(defn is-create-index? [sql_line]
  (re-find #"^(?i)CREATE\s+INDEX\s+" sql_line))

; 1.10、drop index
(defn is-drop-index? [sql_line]
  (re-find #"^(?i)drop\s+INDEX\s+" sql_line))

; 1.11、create dataset
(defn is-create-dataset? [sql_line]
  (re-find #"^(?i)CREATE\s+DATASET\s+" sql_line))

; 1.12、alter dataset
(defn is-alter-dataset? [sql_line]
  (re-find #"^(?i)ALTER\s+DATASET\s+" sql_line))

; 1.13、drop dataset
(defn is-drop-dataset? [sql_line]
  (re-find #"^(?i)DROP\s+DATASET\s+" sql_line))

; 1.14、创建或修改 权限视图
(defn is-create-replace-view? [sql_line]
  (re-find #"^(?i)create\s+or\s+replace\s+view\s+|^(?i)create\s+view\s+|^(?i)replace\s+view\s+" sql_line))

; 1.15、删除 权限视图
(defn is-drop-view? [sql_line]
  (re-find #"^(?i)DROP\s+view\s+" sql_line))


; 直接生成 ast
; 1、select ast
; context：编译上下文
; context = {sql: {sql_type: 'select', query_items: [], table_items: [], where_items: [], order_by_items: [], group_items: [], having_items: [], limit_items: []}, err: [错误列表]}

; select ast
(defn select-ast [[f & rs] context]
  ())

; 预处理字符串
; 1、按空格和特殊字符把字符串转换成数组
; 2、特殊字符：#{"(" ")" "," "=" ">" "<" "+" "-" "*" "/" ">=" "<="}

; 1、按空格和特殊字符把字符串转换成数组
(defn to-back-0
    ([line] (to-back-0 line [] []))
    ([[f & rs] stack-str lst]
     (if (some? f)
         (cond (and (= f \ ) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst)] (to-back-0 rs stack-str [])) (to-back-0 rs stack-str []))
               (and (= f \() (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "("] (to-back-0 rs stack-str [])) (concat ["("] (to-back-0 rs stack-str [])))
               (and (= f \)) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ")"] (to-back-0 rs stack-str [])) (concat [")"] (to-back-0 rs stack-str [])))
               (and (= f \,) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ","] (to-back-0 rs stack-str [])) (concat [","] (to-back-0 rs stack-str [])))
               (and (= f \+) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "+"] (to-back-0 rs stack-str [])) (concat ["+"] (to-back-0 rs stack-str [])))
               (and (= f \-) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "-"] (to-back-0 rs stack-str [])) (concat ["-"] (to-back-0 rs stack-str [])))
               (and (= f \*) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "*"] (to-back-0 rs stack-str [])) (concat ["*"] (to-back-0 rs stack-str [])))
               (and (= f \/) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "/"] (to-back-0 rs stack-str [])) (concat ["/"] (to-back-0 rs stack-str [])))
               (and (= f \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "="] (to-back-0 rs stack-str [])) (concat ["="] (to-back-0 rs stack-str [])))
               (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">="] (to-back-0 (rest rs) stack-str [])) (concat [">="] (to-back-0 (rest rs) stack-str [])))
               (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">"] (to-back-0 rs stack-str [])) (concat [">"] (to-back-0 rs stack-str [])))
               (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<="] (to-back-0 (rest rs) stack-str [])) (concat ["<="] (to-back-0 (rest rs) stack-str [])))
               (and (= f \<) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<"] (to-back-0 rs stack-str [])) (concat ["<"] (to-back-0 rs stack-str [])))

               (and (= f \") (= (count stack-str) 0)) (to-back-0 rs (conj stack-str [f "双"]) lst)
               (and (= f \") (> (count stack-str) 0)) (let [t (peek stack-str)]
                                                          (cond (= (nth t 1) "双") (to-back-0 rs (pop stack-str) lst)
                                                                :else
                                                                (to-back-0 rs stack-str (conj lst f)))
                                                          )
               (and (= f \') (= (count stack-str) 0)) (to-back-0 rs (conj stack-str [f "单"]) lst)
               (and (= f \') (> (count stack-str) 0)) (let [t (peek stack-str)]
                                                          (cond (= (nth t 1) "单") (to-back-0 rs (pop stack-str) lst)
                                                                :else
                                                                (to-back-0 rs stack-str (conj lst f)))
                                                          )
               :else (to-back-0 rs stack-str (conj lst f))
               )
         (if (> (count lst) 0) [(str/join lst)])
         )))

(defn to-back
    ([line] (to-back line [] []))
    ([[f & rs] stack-str lst]
     (if (some? f)
         (cond (and (= f \space) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst)] (to-back rs stack-str [])) (to-back rs stack-str []))
               (and (= f \() (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "("] (to-back rs stack-str [])) (concat ["("] (to-back rs stack-str [])))
               (and (= f \)) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ")"] (to-back rs stack-str [])) (concat [")"] (to-back rs stack-str [])))
               (and (= f \,) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ","] (to-back rs stack-str [])) (concat [","] (to-back rs stack-str [])))
               (and (= f \+) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "+"] (to-back rs stack-str [])) (concat ["+"] (to-back rs stack-str [])))
               (and (= f \-) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "-"] (to-back rs stack-str [])) (concat ["-"] (to-back rs stack-str [])))
               (and (= f \*) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "*"] (to-back rs stack-str [])) (concat ["*"] (to-back rs stack-str [])))
               (and (= f \/) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "/"] (to-back rs stack-str [])) (concat ["/"] (to-back rs stack-str [])))
               (and (= f \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "="] (to-back rs stack-str [])) (concat ["="] (to-back rs stack-str [])))
               (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">="] (to-back (rest rs) stack-str [])) (concat [">="] (to-back (rest rs) stack-str [])))
               (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">"] (to-back rs stack-str [])) (concat [">"] (to-back rs stack-str [])))
               (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<="] (to-back (rest rs) stack-str [])) (concat ["<="] (to-back (rest rs) stack-str [])))
               (and (= f \<) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<"] (to-back rs stack-str [])) (concat ["<"] (to-back rs stack-str [])))

               (and (= f \") (= (count stack-str) 0)) (to-back rs (conj stack-str [f "双"]) (conj lst f))
               (and (= f \") (> (count stack-str) 0)) (let [t (peek stack-str)]
                                                          (cond (= (nth t 1) "双") (to-back rs (pop stack-str) (conj lst f))
                                                                :else
                                                                (to-back rs stack-str (conj lst f)))
                                                          )
               (and (= f \') (= (count stack-str) 0)) (to-back rs (conj stack-str [f "单"]) (conj lst f))
               (and (= f \') (> (count stack-str) 0)) (let [t (peek stack-str)]
                                                          (cond (= (nth t 1) "单") (to-back rs (pop stack-str) (conj lst f))
                                                                :else
                                                                (to-back rs stack-str (conj lst f)))
                                                          )
               (= f \newline) (to-back rs stack-str lst)
               :else (to-back rs stack-str (conj lst f))
               )
         (if (> (count lst) 0) [(str/join lst)])
         )))

; 按逗号切分 query
(defn query-items-line [[f & rs]] ())

;(println (to-back-0 "select sum(t.FY_COMM1)/3 as fyc_sum from n_sal_info t where t.POL_YR = {pol_yr} and t.POL_MONTH in(7,8,9) ' wu da (123) fu ' and t.AGENTNO = {agentno}"))
;(println (to-back-0 "  = {pol_yr} and t.POL_MONTH in(7,8,9) ' wu da (123) fu ' and t.AGENTNO = {agentno}"))

(defn replace-n [[f & rs]]
    (if (= f \newline)
        (println "OK")))

(def line "\n\n\n              (select emp_name from staff_info where empno=a.empno) as emp_name,\n                            c.description as description,\n                                           a.region_code\n                                      from lcs_dept_hiberarchy_trace b,\n                                           agent_info a,\n                                           agent_rank_tbl c,\n                                           (select emp_name from staff_info where empno=a.empno) as d\n                                     where a.empno = {c_empno}\n                                       and exists (select emp_name from staff_info where empno=a.empno)\n                                       GROUP BY b.authorid  HAVING my_count = 2 \n                                       order by c.region_grade desc, a.age asc  LIMIT 0, 2")
(replace-n line)















































