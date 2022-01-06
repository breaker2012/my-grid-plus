(ns org.gridgain.plus.tools.my-lexical
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (java.lang String Double Float Integer Boolean)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyLexical
        ; 是否生成 class 的 main 方法
        :main false
        ))

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

; 判断两个字符串是否相等
(defn is-eq? [s-one s-two]
    (= (str/lower-case (str/trim s-one)) (str/lower-case (str/trim s-two))))

;(defn is-eq? [s-one s-two]
;    (re-find (re-pattern (str "^(?i)\\s*" s-one "\\s*$")) s-two))

; 预处理字符串
; 1、按空格和特殊字符把字符串转换成数组
; 2、特殊字符：#{"(" ")" "," "=" ">" "<" "+" "-" "*" "/" ">=" "<="}

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

; 按逗号切分 query
; stack 记录 （）
; lst 记录 query-item
(defn query-items-line
    ([lst] (query-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0)
                                                       (let [{query-items :query-items rs-lst :rs-lst} (query-items-line rs stack [])]
                                                           {:query-items (concat [lst] query-items) :rs-lst rs-lst})
                                                       )

               (and (is-eq? f "from") (= (count stack) 0)) (if (> (count lst) 0)
                                                               {:query-items [lst] :rs-lst rs}
                                                               )
               (= f "(") (query-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (query-items-line rs (pop stack) (conj lst f)))
               :else
               (query-items-line rs stack (conj lst f))
               ))))

; 获取 table 的定义
(defn tables-items-line
    ([lst] (tables-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0)
                                                       (let [{query-items :table-items rs-lst :rs-lst} (tables-items-line rs stack [])]
                                                           {:table-items (concat [lst] query-items) :rs-lst rs-lst})
                                                       )

               (and (is-eq? f "where") (= (count stack) 0)) (if (> (count lst) 0)
                                                                {:table-items [lst] :rs-lst rs}
                                                                )
               (= f "(") (tables-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (tables-items-line rs (pop stack) (conj lst f)))
               :else
               (tables-items-line rs stack (conj lst f))
               ))))

; 获取 where 的定义
(defn where-items-line
    ([lst] (where-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "group") (some? (first rs)) (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:where-items lst :rs-lst (concat ["group"] rs)})
               (and (is-eq? f "order") (some? (first rs)) (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:where-items lst :rs-lst (concat ["order"] rs)})
               (and (is-eq? f "limit") (= (count stack) 0)) (if (> (count lst) 0) {:where-items lst :rs-lst (concat ["limit"] rs)})
               (= f "(") (where-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (where-items-line rs (pop stack) (conj lst f)))
               :else
               (where-items-line rs stack (conj lst f))
               )
         (if (> (count lst) 0) {:where-items lst :rs-lst []}))))

; 获取 group by
; 如果在 group by 字段中有 having 就返回 {:group-by [] :having true :rs-lst rs}
(defn group-by-items-line
    ([lst] (group-by-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "having") (= (count stack) 0) (> (count lst) 0)) {:group-by lst :having true :rs-lst rs}
               (= f "(") (group-by-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (group-by-items-line rs (pop stack) (conj lst f)))
               :else
               (group-by-items-line rs stack (conj lst f))
               )
         )))

; 返回： {:having [] :rs-lst []}
(defn group-by-having-items-line
    ([lst] (group-by-having-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "order") (some? (first rs)) (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:group-having lst :rs-lst (concat ["order"] rs)})
               (and (is-eq? f "limit") (= (count stack) 0)) (if (> (count lst) 0) {:group-having lst :rs-lst (concat ["limit"] rs)})
               (= f "(") (group-by-having-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (group-by-having-items-line rs (pop stack) (conj lst f)))
               :else
               (group-by-having-items-line rs stack (conj lst f))
               )
         (if (and (> (count lst) 0) (= (count stack) 0)) {:group-having lst :rs-lst []})
         )))

; group by
(defn sql-group-by [lst]
    (let [m (group-by-items-line lst)]
        (if (empty? m)
            (let [{group-having :group-having rs-lst :rs-lst} (group-by-having-items-line lst)]
                {:group-by group-having :rs-lst rs-lst})
            (let [{group-having :group-having rs-lst :rs-lst} (group-by-having-items-line (get m :rs-lst))]
                {:group-by (get m :group-by) :having group-having :rs-lst rs-lst}))))

; 获取 order by
; 返回： {:order-by-obj {:order-by [] :desc-asc "desc"} :rs-lst []}
; 如果返回为 nil 证明里面没有 order by
(defn order-by-items-line
    ([lst] (order-by-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "limit") (= (count stack) 0)) (if (> (count lst) 0) {:order lst :rs-lst (concat ["limit"] rs)})
               (= f "(") (order-by-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (order-by-items-line rs (pop stack) (conj lst f)))
               :else
               (order-by-items-line rs stack (conj lst f))
               )
         (if (and (> (count lst) 0) (= (count stack) 0)) {:order lst :rs-lst []})
         )))

; 获取 group by, order by, limit
(defn where-extend-line [[f & rs]]
    (if (some? f)
        (cond (and (is-eq? f "group") (is-eq? (first rs) "by"))
              (let [{group-by :group-by having :having rs-lst :rs-lst} (sql-group-by (rest rs))]
                  (let [m (where-extend-line rs-lst)]
                      (if (some? having) (assoc m :group-by group-by :having having) (assoc m :group-by group-by))))

              (and (is-eq? f "order") (is-eq? (first rs) "by"))
              (let [{order :order rs-lst :rs-lst} (order-by-items-line (rest rs))]
                  (let [m (where-extend-line rs-lst)] (assoc m :order-by order)))

              (is-eq? f "limit")
              {:limit rs}

              :else
              {:err "where 后面字符串错误"}
              )))

; 获取 select 语句对应的字符
(defn get-segments [select-sql]
    (let [lst (to-back select-sql)]
        (let [{query-items :query-items rs-lst-query :rs-lst} (query-items-line lst)]
            (if (some? rs-lst-query)
                (let [{table-items :table-items rs-lst-tables :rs-lst} (tables-items-line rs-lst-query)]
                    (if (some? rs-lst-tables)
                        (let [{where-items :where-items rs-lst-where :rs-lst} (where-items-line rs-lst-tables)]
                            (if (some? rs-lst-where) (let [{group-by :group-by having :having order-by :order-by limit :limit} (where-extend-line rs-lst-where)]
                                                         {:query-items query-items :table-items table-items :where-items where-items :group-by group-by :having having :order-by order-by :limit limit})
                                                     {:query-items query-items :table-items table-items :where-items where-items}))
                        {:query-items query-items :table-items table-items}))))
        ))

;(def line "select \n                            (select emp_name from staff_info where empno=a.empno) as emp_name,\n                            c.description as description,\n                                           a.region_code\n                                      from lcs_dept_hiberarchy_trace b,\n                                           agent_info a,\n                                           agent_rank_tbl c,\n                                           (select emp_name from staff_info where empno=a.empno) as d\n                                     where a.empno = {c_empno}\n                                       and exists (select emp_name from staff_info where empno=a.empno)\n                                       GROUP BY b.authorid  HAVING my_count = 2 \n                                       order by c.region_grade desc, a.age asc  LIMIT 0, 2")
;(println (get-segments line))

(defn get_line
    ([lst] (get_line lst [] []))
    ([[f & r] stack lst]
     (if (some? f)
         (cond (contains? #{\[ \] \{ \}} f) (if-not (empty? stack)
                                                (recur r [] (concat lst [(str/join stack) (.toString f)]))
                                                (recur r [] (concat lst [(.toString f)])))
               :else
               (recur r (conj stack (.toString f)) lst)
               )
         (if (empty? stack)
             lst
             (concat lst [(str/join stack)])))))

(defn get_trans_lst [lst]
    (if (and (= (first lst) "{") (= (last lst) "}"))
        (loop [i 1 rs []]
            (if (< (+ i 1) (count lst))
                (recur (+ i 1) (concat rs (get_line (nth lst i))))
                rs))))

(defn tran_segment
    ([lst] (tran_segment (get_trans_lst lst) [] [] [] [] [] [] [] [] [] [] [] [] []))
    ([[f & r] stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch]
     (if (some? f)
         (cond (my-lexical/is-eq? f "(") (cond (not (empty? stack_name)) (recur r (conj stack_small f) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f ")") (cond (not (empty? stack_name)) (recur r (pop stack_small) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "[") (cond (not (empty? stack_name)) (recur r stack_small (conj stack_mid f) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "]") (cond (not (empty? stack_name)) (recur r stack_small (pop stack_mid) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "{") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (conj stack_big f) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "}") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (pop stack_big) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )

               (re-find #"^(?i)name\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                       (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                  (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                      (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                                                                                                      (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                   (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                      (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                                                                                                      (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                                                                                                   (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                       (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                                                                                                       (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                                                                                                   (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                       (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                                                                                                       (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                       :else
                                                       (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)params\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                         (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                        (recur r stack_small stack_mid stack_big [] stack_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                                                                                                        (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                     (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                                                                                                     (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                         (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                                                                                                         (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                                                                                                     (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                         (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                                                                                                         (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                         :else
                                                         (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)trans\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                        (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                       (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                                                                                                       (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                       (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                                                                                                       (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans [] stack_descrip stack_batch lst_batch)
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip [] stack_batch)
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                        :else
                                                        (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)descrip\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                          (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                         (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                                                                                                         (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                         (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                                                                                                         (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                                                                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                                                                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip [] stack_batch)
                                                                                                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                          :else
                                                          (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)is_batch\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                        (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                       (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                                                                                                       (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                       (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                                                                                                       (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                                                                                                    (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip (conj stack_batch f) lst_batch)
                                                                                                                                        (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                        :else
                                                        (throw (Exception. "tran 语句错误！")))
               :else
               (cond (not (empty? stack_name)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_params)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_trans)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_descrip)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                     (not (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                     )
               )
         (letfn [(get_vs [[f & r]]
                     (if (and (some? r) (= (last r) ","))
                         (reverse (rest (reverse r)))
                         r))
                 (get_value [stack lst]
                     (cond (and (not (empty? stack)) (empty? lst)) stack
                           (and (not (empty? lst)) (empty? stack)) lst
                           :else
                           lst
                           ))]
             (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                 {:name (get_vs (get_value stack_name lst_name)) :params (get_vs (get_value stack_params lst_params)) :trans (get_vs (get_value stack_trans lst_trans)) :descrip (get_vs (get_value stack_descrip lst_descrip)) :is_batch (get_vs (get_value stack_batch lst_batch))}
                 (throw (Exception. "tran 语句错误！"))))
         )))

; scenes 的 segment
(defn scenes_segment
    ([lst] (scenes_segment (get_trans_lst lst) [] [] [] [] [] [] [] [] [] [] [] [] []))
    ([[f & r] stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch]
     (if (some? f)
         (cond (my-lexical/is-eq? f "(") (cond (not (empty? stack_name)) (recur r (conj stack_small f) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f ")") (cond (not (empty? stack_name)) (recur r (pop stack_small) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "[") (cond (not (empty? stack_name)) (recur r stack_small (conj stack_mid f) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "]") (cond (not (empty? stack_name)) (recur r stack_small (pop stack_mid) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "{") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (conj stack_big f) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "}") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (pop stack_big) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )

               (re-find #"^(?i)name\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                       (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                       :else
                                                       (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)params\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                         (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big [] stack_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                         :else
                                                         (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)sql\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                        (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans [] stack_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip [] stack_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                        :else
                                                        (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)descrip\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                          (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip [] stack_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                          :else
                                                          (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)is_batch\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                        (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                            (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip (conj stack_batch f) lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                        :else
                                                        (throw (Exception. "tran 语句错误！")))
               :else
               (cond (not (empty? stack_name)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_params)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_trans)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_descrip)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                     (not (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                     )
               )
         (letfn [(get_vs [[f & r]]
                     (if (and (some? r) (= (last r) ","))
                         (reverse (rest (reverse r)))
                         r))
                 (get_value [stack lst]
                     (cond (and (not (empty? stack)) (empty? lst)) stack
                           (and (not (empty? lst)) (empty? stack)) lst
                           :else
                           lst
                           ))]
             (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                 {:name (get_vs (get_value stack_name lst_name)) :params (get_vs (get_value stack_params lst_params)) :sql (get_vs (get_value stack_trans lst_trans)) :descrip (get_vs (get_value stack_descrip lst_descrip)) :is_batch (get_vs (get_value stack_batch lst_batch))}
                 (throw (Exception. "tran 语句错误！"))))
         )))

; cron 的 segment
(defn cron_segment
    ([lst] (cron_segment (get_trans_lst lst) [] [] [] [] [] [] [] [] [] [] [] [] []))
    ([[f & r] stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch]
     (if (some? f)
         (cond (my-lexical/is-eq? f "(") (cond (not (empty? stack_name)) (recur r (conj stack_small f) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f ")") (cond (not (empty? stack_name)) (recur r (pop stack_small) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "[") (cond (not (empty? stack_name)) (recur r stack_small (conj stack_mid f) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "]") (cond (not (empty? stack_name)) (recur r stack_small (pop stack_mid) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "{") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (conj stack_big f) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "}") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (pop stack_big) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )

               (re-find #"^(?i)name\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                       (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                       :else
                                                       (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)params\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                         (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big [] stack_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                         :else
                                                         (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)batch\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                      (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                          (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                      (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                      (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans [] stack_descrip stack_batch lst_batch)
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                      (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip [] stack_batch)
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                      :else
                                                      (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)descrip\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                          (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip [] stack_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                          :else
                                                          (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)cron\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                           (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                           :else
                                                           (throw (Exception. "tran 语句错误！")))
               :else
               (cond (not (empty? stack_name)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_params)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_trans)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_descrip)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                     (not (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                     )
               )
         (letfn [(get_vs [[f & r]]
                     (if (and (some? r) (= (last r) ","))
                         (reverse (rest (reverse r)))
                         r))
                 (get_value [stack lst]
                     (cond (and (not (empty? stack)) (empty? lst)) stack
                           (and (not (empty? lst)) (empty? stack)) lst
                           :else
                           lst
                           ))]
             (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                 {:name (get_vs (get_value stack_name lst_name)) :params (get_vs (get_value stack_params lst_params)) :batch (get_vs (get_value stack_trans lst_trans)) :descrip (get_vs (get_value stack_descrip lst_descrip)) :cron (get_vs (get_value stack_batch lst_batch))}
                 (throw (Exception. "tran 语句错误！"))))
         )))
















































