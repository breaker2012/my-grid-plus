(ns org.gridgain.plus.dml.my-insert
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.tools.my-util :as my-util]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite)
             (org.apache.ignite.binary BinaryObjectBuilder)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.util List ArrayList))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyInsert
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_call_scenes [org.apache.ignite.Ignite Long clojure.lang.PersistentArrayMap java.util.ArrayList] Object]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 在函数内部处理 ,
(defn my-comma-fn
    ([lst] (my-comma-fn lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (concat stack [f]) (concat lst [f]) result-lst)
               (= f ")") (recur rs (pop stack) (concat lst [f]) result-lst)
               :else
               (recur rs stack (concat lst [f]) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

; 具体过程：
; 1、获取 insert obj 和 insert view obj 两个对象
; 2、通过 group_id 判断这个语句是在：实时数据集中执行还是批处理数据集
; 3、如果是实时数据集，就要判断是否需要记录 log
; 4、在批处理数据集中，就要判断是否来至与实时树集中的表

;(defn get-insert-items
;    ([tokens] (when-let [m (get-insert-items tokens [] [])]
;                  (if (and (= (count m) 2) (= (count (nth m 0)) (count (nth m 1))))
;                      (letfn [(to-kv [n lst1 lst2 lst_kv]
;                                  (if (> n -1)
;                                      (if (not (my-lexical/is-eq? (nth lst1 n) (nth lst2 n)))
;                                          (recur (dec n) lst1 lst2 (concat lst_kv [{:item_name (nth lst1 n) :item_value (nth lst2 n)}]))
;                                          (recur (dec n) lst1 lst2 lst_kv)) lst_kv))]
;                          (to-kv (dec (count (nth m 0))) (nth m 0) (nth m 1) [])))))
;    ([[f & r] stack lst]
;     (if (some? f)
;         (cond (and (my-lexical/is-eq? f "values") (> (count stack) 0)) (recur r [] stack)
;               :else
;               (recur r (conj stack f) lst)
;               )
;         (if (and (> (count stack) 0) (> (count lst) 0)) [lst stack]))))

(defn get-item-line [[f & r]]
    (if (and (= f "(") (= (last r) ")"))
        (reverse (rest (reverse r)))
        (throw (Exception. "insert 语句错误！"))))

(defn get-insert-items
    ([tokens] (when-let [[columns items] (get-insert-items tokens [] [])]
                  (let [my-columns (my-comma-fn (get-item-line columns))  my-items (my-comma-fn (get-item-line items))]
                      (loop [[f_c & r_c] my-columns [f_m & r_m] my-items lst_kv []]
                          (if (and (some? f_c) (some? f_m))
                              (if-not (= (first f_c) \,)
                                  (recur r_c r_m (concat lst_kv [{:item_name (first f_c) :item_value f_m}]))
                                  (recur r_c r_m lst_kv))
                              lst_kv)))))
    ([[f & r] stack lst]
     (if (some? f)
         (cond (and (my-lexical/is-eq? f "values") (> (count stack) 0)) (recur r [] stack)
               :else
               (recur r (concat stack [f]) lst)
               )
         (if (and (> (count stack) 0) (> (count lst) 0)) [lst stack]))))

(defn get_insert_obj_lst [^clojure.lang.PersistentVector lst]
    (letfn [(insert_obj [[f & r]]
                (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
                    (if-let [items (get-insert-items (rest (rest r)))]
                        {:table_name (str/lower-case (second r)) :values items}
                        (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！"))
                        )
                    ))]
        (insert_obj lst)))

; 获取 inset_obj
; 例如："INSERT INTO categories (categoryid, categoryname, description) values (12, 'wudafu', 'meiyy')"
; {:table_name "categories",
;  :values ({:item_name "description", :item_value "'meiyy'"}
;           {:item_name "categoryname", :item_value "'wudafu'"}
;           {:item_name "categoryid", :item_value "12"})}
(defn get_insert_obj [^String line]
    (if-let [lst (my-lexical/to-back line)]
        (letfn [(insert_obj [[f & r]]
                    (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
                        (if-let [items (get-insert-items (rest (rest r)))]
                            {:table_name (str/lower-case (second r)) :values items}
                            (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！"))
                            ;(if (my-lexical/is-eq? (first (rest (rest r))) "values")
                            ;    (letfn [(get_columns [^Ignite ignite ^String table_name]
                            ;                (loop [[f & r] (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.column_name from table_item as m join my_meta_tables as t on m.table_id = t.id where t.table_name = ?") (to-array [table_name]))))
                            ;                       lst []]
                            ;                    (if (some? f)
                            ;                        (recur r (conj lst (nth f 0)))
                            ;                        lst)
                            ;                    ))
                            ;            (get_items [lst]
                            ;                (if (and (my-lexical/is-eq? (first lst) "(") (my-lexical/is-eq? (last lst) ")"))
                            ;                    (loop [[f & r] (reverse (rest (reverse (rest lst)))) lst_items []]
                            ;                        (if (some? f)
                            ;                            (if-not (my-lexical/is-eq? f ",")
                            ;                                (recur r (conj lst_items f))
                            ;                                (recur r lst_items))
                            ;                            lst_items))))
                            ;            (to-kv [n lst1 lst2 lst_kv]
                            ;                (if (> n -1)
                            ;                    (if (not (my-lexical/is-eq? (nth lst1 n) (nth lst2 n)))
                            ;                        (recur (dec n) lst1 lst2 (concat lst_kv [{:item_name (nth lst1 n) :item_value (nth lst2 n)}]))
                            ;                        (recur (dec n) lst1 lst2 lst_kv)) lst_kv))]
                            ;        (let [columns (get_columns ignite (str/lower-case (second r))) items (get_items (rest (rest (rest r))))]
                            ;            {:table_name (str/lower-case (second r)) :values (to-kv (dec (count columns)) columns items [])})
                            ;        ))
                            )
                        ))]
            (insert_obj lst))))

(defn get_insert_obj_fun [^String line ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [lst (my-lexical/to-back line)]
        (letfn [(insert_obj [[f & r]]
                    (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
                        (if-let [items (get-insert-items (rest (rest r)))]
                            {:table_name (str/lower-case (second r)) :values items}
                            ;(if (my-lexical/is-eq? (first (rest (rest r))) "values")
                            ;    (letfn [(get_columns [^Ignite ignite ^String table_name]
                            ;                (loop [[f & r] (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.column_name from table_item as m join my_meta_tables as t on m.table_id = t.id where t.table_name = ?") (to-array [table_name]))))
                            ;                       lst []]
                            ;                    (if (some? f)
                            ;                        (recur r (conj lst (nth f 0)))
                            ;                        lst)
                            ;                    ))
                            ;            (get_items [lst]
                            ;                (if (and (my-lexical/is-eq? (first lst) "(") (my-lexical/is-eq? (last lst) ")"))
                            ;                    (loop [[f & r] (reverse (rest (reverse (rest lst)))) lst_items []]
                            ;                        (if (some? f)
                            ;                            (if-not (my-lexical/is-eq? f ",")
                            ;                                (recur r (conj lst_items f))
                            ;                                (recur r lst_items))
                            ;                            lst_items))))
                            ;            (to-kv [n lst1 lst2 lst_kv]
                            ;                (if (> n -1)
                            ;                    (if (not (my-lexical/is-eq? (nth lst1 n) (nth lst2 n)))
                            ;                        (if-not (and (= (first (nth lst1 n)) \:) (contains? dic_paras (str/join (rest (nth lst1 n)))))
                            ;                            (recur (dec n) lst1 lst2 (concat lst_kv [{:item_name (nth lst1 n) :item_value (nth lst2 n)}]))
                            ;                            (throw (Exception. (format "列不能作为参数！%s" (nth lst1 n)))))
                            ;                        (recur (dec n) lst1 lst2 lst_kv))
                            ;                    lst_kv))]
                            ;        (let [columns (get_columns ignite (str/lower-case (second r))) items (get_items (rest (rest (rest r))))]
                            ;            {:table_name (str/lower-case (second r)) :values (to-kv (dec (count columns)) columns items [])})
                            ;        ))
                            )
                        ))]
            (insert_obj lst))))

; 例如：从 my_insert_views 中获取 code = "INSERT INTO City(Name, District)"
; 转换为 view_obj :
; {:table_name "City", :lst #{"District" "Name"}}
(defn get_view_obj [^Ignite ignite ^Long group_id ^String table_name]
    (if-let [lst_rs (first (.getAll (.query (.cache ignite "my_insert_views") (.setArgs (SqlFieldsQuery. "select m.code from my_insert_views as m join my_group_view as v on m.id = v.view_id where m.table_name = ? and v.my_group_id = ? and v.view_type = ?") (to-array [table_name group_id "增"])))))]
        (if (some? lst_rs)
            (letfn [(get_insert_view_items [[f & r] lst]
                        (if (some? f)
                            (if (not (or (= f "(") (= f ")") (= f ",")))
                                (recur r (concat lst [f]))
                                (recur r lst))
                            lst))
                    (get_insert_view_obj [[f & r]]
                        (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
                            (if-let [items (get_insert_view_items (rest (rest r)) #{})]
                                {:table_name (second r) :lst items})
                            ))]
                (get_insert_view_obj (my-lexical/to-back (nth lst_rs 0))))
            )
        ))

; 获取表中 PK列 和 数据列
; 输入表名获取 "Categories"
; {:pk ({:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false}),
; :data ({:column_name "categoryname", :column_type "varchar", :pkid false, :auto_increment false}
;        {:column_name "description", :column_type "varchar", :pkid false, :auto_increment false}
;        {:column_name "picture", :column_type "varchar", :pkid false, :auto_increment false})}
(defn get_pk_data [^Ignite ignite ^String table_name]
    (when-let [it (.iterator (.query (.cache ignite "my_meta_tables") (.setArgs (doto (SqlFieldsQuery. "select m.column_name, m.column_type, m.pkid, m.auto_increment from table_item as m join my_meta_tables as t on m.table_id = t.id where t.table_name = ?")
                                                                                    (.setLazy true)) (to-array [table_name]))))]
        (letfn [
                ; 从 iterator 中获取 pk列和数据列
                (get_pk_data_it [it lst_pk lst_data]
                    (if (.hasNext it)
                        (when-let [row (.next it)]
                            (if (true? (.get row 2))
                                (recur it (concat lst_pk [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]) lst_data)
                                (recur it lst_pk (concat lst_data [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]))
                                ))
                        {:pk lst_pk :data lst_data}))]
            (get_pk_data_it it [] []))))

; 获取 insert obj 和 insert view obj 两个对象
; insert obj: get_insert_obj
; insert view obj: get_view_obj

; pk_data: (get_pk_data ignite "Categories")
; insert_obj: (get_insert_obj ignite "insert into Categories values(1,'Beverages','Soft drinks, coffees, teas, beers, and ales', '')")
(defn get_pk_data_with_data [pk_data insert_obj]
    (let [{values :values} insert_obj]
        (letfn [(
                    ; 第一个参数是：
                    ; :values ({:item_name "picture", :item_value "''"}
                    ;           {:item_name "description", :item_value "'Soft drinks, coffees, teas, beers, and ales'"}
                    ;           {:item_name "categoryname", :item_value "'Beverages'"}
                    ;           {:item_name "categoryid", :item_value "1"})
                    ; 第二个参数是：
                    ; column：{:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false}
                    get_value [[f & r] column]
                    (if (some? f)
                        (if (my-lexical/is-eq? (-> column :column_name) (-> f :item_name))
                            (assoc column :item_value (-> f :item_value))
                            (recur r column)))
                    )
                (
                    ; 第一个参数是：({:column_name "categoryname", :column_type "varchar", :pkid false, :auto_increment false}
                    ;         {:column_name "description", :column_type "varchar", :pkid false, :auto_increment false}
                    ;         {:column_name "picture", :column_type "varchar", :pkid false, :auto_increment false})
                    ; 第二个参数是： :values ({:item_name "picture", :item_value "''"}
                    ;           {:item_name "description", :item_value "'Soft drinks, coffees, teas, beers, and ales'"}
                    ;           {:item_name "categoryname", :item_value "'Beverages'"}
                    ;           {:item_name "categoryid", :item_value "1"})
                    ; 返回的结果：({:column_name "categoryname", :column_type "varchar", :item_value "'Beverages'", :pkid false, :auto_increment false}
                    ;         {:column_name "description", :column_type "varchar", :item_value "'Soft drinks, coffees, teas, beers, and ales'", :pkid false, :auto_increment false}
                    ;         {:column_name "picture", :column_type "varchar", :item_value nil, :pkid false, :auto_increment false})
                    get_rs
                    ([lst_column values] (get_rs lst_column values []))
                    ([[f & r] values lst]
                     (if (some? f)
                         (let [m (get_value values f)]
                             (if-not (nil? m)
                                 (recur r values (concat lst [m]))
                                 (recur r values (concat lst [(assoc m :item_value nil)]))))
                         lst))
                    )] {:pk_rs (get_rs (-> pk_data :pk) values) :data_rs (get_rs (-> pk_data :data) values)})
        ))

; {:table_name "categories",
;  :values ({:item_name "description", :item_value "'meiyy'"}
;           {:item_name "categoryname", :item_value "'wudafu'"}
;           {:item_name "categoryid", :item_value "12"})}
(defn insert_users_group [^Ignite ignite ^clojure.lang.PersistentArrayMap insert_obj]
    (letfn [(user_group_sql [^clojure.lang.PersistentArrayMap insert_obj]
                (if-let [{table_name :table_name values :values} insert_obj]
                    (loop [[f & r] values sb_item (StringBuilder.) sb_vs (StringBuilder.)]
                        (if (some? f)
                            (if (contains? #{"group_name" "data_set_id" "group_type"} (str/lower-case (-> f :item_name)))
                                (recur r (doto sb_item (.append (format "%s," (-> f :item_name)))) (doto sb_vs (.append (format "%s," (-> f :item_value)))))
                                (if (false? (my-lexical/is-eq? (-> f :item_name) "id"))
                                    (throw (Exception. (format "插入数据的列 %s 在 my_users_group 表中不存在！" (-> f :item_name))))
                                    (recur r sb_item sb_vs)))
                            (format "insert into %s (%s id) values (%s ?)" table_name (.toString sb_item) (.toString sb_vs)))
                        )))]
        (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. (user_group_sql insert_obj)) (to-array [(.incrementAndGet (.atomicSequence ignite "my_users_group" 0 true))])))))
    )

; {:table_name "my_dataset",
;  :values ({:item_name "id", :item_value "1"}
;           {:item_name "is_real", :item_value "1"}
;           {:item_name "dataset_name", :item_value "'real_ds'"})}
(defn insert_data_set [^Ignite ignite ^clojure.lang.PersistentArrayMap insert_obj]
    (letfn [(data_set_sql [^clojure.lang.PersistentArrayMap insert_obj]
                (if-let [{table_name :table_name values :values} insert_obj]
                    (loop [[f & r] values sb_item (StringBuilder.) sb_vs (StringBuilder.)]
                        (if (some? f)
                            (if (contains? #{"is_real" "dataset_name"} (str/lower-case (-> f :item_name)))
                                (recur r (doto sb_item (.append (format "%s," (-> f :item_name)))) (doto sb_vs (.append (format "%s," (-> f :item_value)))))
                                (if (false? (my-lexical/is-eq? (-> f :item_name) "id"))
                                    (throw (Exception. (format "插入数据的列 %s 在 my_dataset 表中不存在！" (-> f :item_name))))
                                    (recur r sb_item sb_vs)))
                            (format "insert into %s (%s id) values (%s ?)" table_name (.toString sb_item) (.toString sb_vs)))
                        )))]
        (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. (data_set_sql insert_obj)) (to-array [(.incrementAndGet (.atomicSequence ignite "my_dataset" 0 true))])))))
    )

; 添加额外的 pk
; pk_rs: [{:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false, :item_value "1"}]
(defn get_plus_pk
    ([pk_rs] (get_plus_pk pk_rs []))
    ([[f_pk & r_pk] lst]
     (if (some? f_pk)
         (recur r_pk (concat lst [(assoc f_pk :pkid false) (assoc f_pk :column_name (format "%s_pk" (-> f_pk :column_name)) :pkid true :auto_increment false)]))
         lst)))

; 第一个参数： table_name
; 第二个参数： {:pk_rs [{:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false, :item_value "1"}],
; :data_rs [{:column_name "categoryname",
;            :column_type "varchar",
;            :pkid false,
;            :auto_increment false,
;            :item_value "'Beverages'"}
;           {:column_name "description",
;            :column_type "varchar",
;            :pkid false,
;            :auto_increment false,
;            :item_value "'Soft drinks, coffees, teas, beers, and ales'"}
;           {:column_name "picture", :column_type "varchar", :pkid false, :auto_increment false, :item_value "''"}]}
(defn insert_obj_to_db_no_log [^Ignite ignite ^Long group_id ^String table_name ^clojure.lang.PersistentArrayMap pk_data]
    (let [{pk_rs :pk_rs data_rs :data_rs} pk_data]
        (letfn [(get_binaryObject_pk [^BinaryObjectBuilder builder [f & r]]
                    (if (some? f)
                        (let [key (format "%s_pk" (-> f :column_name)) value (my-lexical/get_jave_vs (-> f :column_type) (my-expression/get_plus_value ignite group_id (-> f :column_type) (-> f :item_value)))]
                            (recur (doto builder (.setField key value)) r))
                        builder))
                (get_binaryObject [^BinaryObjectBuilder builder [f & r]]
                    (if (some? f)
                        (recur (doto builder (.setField (-> f :column_name) (my-lexical/get_jave_vs (-> f :column_type) (my-expression/get_plus_value ignite group_id (-> f :column_type) (-> f :item_value))))) r)
                        builder))
                (get_pk_rs [pk]
                    (cond (= (count pk) 1) (cond (true? (-> (first pk) :auto_increment)) (if (re-find #"^(?i)integer$|^(?i)int$" (-> (first pk) :column_type))
                                                                                             (MyConvertUtil/ConvertToInt (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                                                             (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                 (false? (-> (first pk) :auto_increment)) (my-lexical/get_jave_vs (-> (first pk) :column_type) (my-expression/get_plus_value ignite group_id (-> (first pk) :column_type) (-> (first pk) :item_value)))
                                                 )
                          (> (count pk) 1) (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                                               (get_binaryObject_pk keyBuilder pk)
                                               (throw (Exception. "表不存在主键！")))
                          :else
                          (throw (Exception. "表不存在主键！"))
                          ))
                (get_data_rs [data pk]
                    (if-let [valueBuilder (.builder (.binary ignite) (KvSql/getValueType ignite (format "f_%s" table_name)))]
                        (cond (= (count pk) 1) (get_binaryObject valueBuilder data)
                              (> (count pk) 1) (get_binaryObject valueBuilder (concat (get_plus_pk pk) data))
                              :else
                              (throw (Exception. "表不存在主键！")))
                        (throw (Exception. "表不存在主键！"))))
                ]
            [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) (.build (get_pk_rs pk_rs)) (.build (get_data_rs data_rs)) (SqlType/INSERT))]))
    )

(defn insert_obj_to_db_no_log_fun [^Ignite ignite ^Long group_id ^String table_name ^clojure.lang.PersistentArrayMap pk_data ^clojure.lang.PersistentArrayMap dic_paras]
    (let [{pk_rs :pk_rs data_rs :data_rs} pk_data]
        (letfn [(get_binaryObject_pk [^BinaryObjectBuilder builder [f & r]]
                    (if (some? f)
                        (let [key (format "%s_pk" (-> f :column_name)) value (my-lexical/get_jave_vs (-> f :column_type) (my-expression/plus_value_fun ignite group_id (-> f :column_type) (-> f :item_value) dic_paras))]
                            (recur (doto builder (.setField key value)) r))
                        builder))
                (get_binaryObject [^BinaryObjectBuilder builder [f & r]]
                    (if (some? f)
                        (recur (doto builder (.setField (-> f :column_name) (my-lexical/get_jave_vs (-> f :column_type) (my-expression/plus_value_fun ignite group_id (-> f :column_type) (-> f :item_value) dic_paras)))) r)
                        builder))
                (get_pk_rs [pk]
                    (cond (= (count pk) 1) (cond (true? (-> (first pk) :auto_increment)) (if (re-find #"^(?i)integer$|^(?i)int$" (-> (first pk) :column_type))
                                                                                             (MyConvertUtil/ConvertToInt (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                                                             (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                 (false? (-> (first pk) :auto_increment)) (my-lexical/get_jave_vs (-> (first pk) :column_type) (my-expression/plus_value_fun ignite group_id (-> (first pk) :column_type) (-> (first pk) :item_value) dic_paras))
                                                 )
                          (> (count pk) 1) (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                                               (get_binaryObject_pk keyBuilder pk)
                                               (throw (Exception. "表不存在主键！")))
                          :else
                          (throw (Exception. "表不存在主键！"))
                          ))
                (get_data_rs [data pk]
                    (if-let [valueBuilder (.builder (.binary ignite) (KvSql/getValueType ignite (format "f_%s" table_name)))]
                        (cond (= (count pk) 1) (get_binaryObject valueBuilder data)
                              (> (count pk) 1) (get_binaryObject valueBuilder (concat (get_plus_pk pk) data))
                              :else
                              (throw (Exception. "表不存在主键！")))
                        (throw (Exception. "表不存在主键！"))))
                ]
            [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) (.build (get_pk_rs pk_rs)) (.build (get_data_rs data_rs)) (SqlType/INSERT))]))
    )

(defn insert_obj_to_db [^Ignite ignite ^Long group_id ^String table_name ^clojure.lang.PersistentArrayMap pk_data]
    (let [{pk_rs :pk_rs data_rs :data_rs} pk_data]
        (letfn [(get_binaryObject_pk [^BinaryObjectBuilder builder [f & r] ^List lst]
                    (if (some? f)
                        (let [key (format "%s_pk" (-> f :column_name)) value (my-lexical/get_jave_vs (-> f :column_type) (my-expression/plus_value ignite group_id (-> f :column_type) (-> f :item_value)))]
                            (recur (doto builder (.setField key value)) r (doto lst (.add (MyKeyValue. key value)))))
                        [builder lst]))
                (get_binaryObject [^BinaryObjectBuilder builder [f & r] ^List lst]
                    (if (some? f)
                        (let [key (-> f :column_name) value (my-lexical/get_jave_vs (-> f :column_type) (my-expression/plus_value ignite group_id (-> f :column_type) (-> f :item_value)))]
                            (recur (doto builder (.setField key value)) r (doto lst (.add (MyKeyValue. key value)))))
                        [builder lst]))
                (get_pk_rs [pk]
                    (cond (= (count pk) 1) (cond (true? (-> (first pk) :auto_increment)) (if (re-find #"^(?i)integer$|^(?i)int$" (-> (first pk) :column_type))
                                                                                             (MyConvertUtil/ConvertToInt (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                                                             (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                 (false? (-> (first pk) :auto_increment)) (my-lexical/get_jave_vs (-> (first pk) :column_type) (my-expression/plus_value ignite group_id (-> (first pk) :column_type) (-> (first pk) :item_value)))
                                                 )
                          (> (count pk) 1) (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                                               (get_binaryObject_pk keyBuilder pk (ArrayList.))
                                               (throw (Exception. "表不存在主键！")))
                          :else
                          (throw (Exception. "表不存在主键！"))
                          ))
                (get_data_rs [data pk]
                    (if-let [valueBuilder (.builder (.binary ignite) (KvSql/getValueType ignite (format "f_%s" table_name)))]
                        (cond (= (count pk) 1) (get_binaryObject valueBuilder data (ArrayList.))
                              (> (count pk) 1) (get_binaryObject valueBuilder (concat (get_plus_pk pk) data) (ArrayList.))
                              :else
                              (throw (Exception. "表不存在主键！")))
                        (throw (Exception. "表不存在主键！"))))
                ]
            (let [log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true)) pk (get_pk_rs pk_rs) data (get_data_rs data_rs pk_rs)]
                (if (vector? pk)
                    [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) (.build (nth pk 0)) (.build (nth data 0)) (SqlType/INSERT))
                     (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) (nth pk 1) (nth data 1) (SqlType/INSERT)))) (SqlType/INSERT))]
                    [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) pk (.build (nth data 0)) (SqlType/INSERT))
                     (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) pk (nth data 1) (SqlType/INSERT)))) (SqlType/INSERT))])
                )
            ))
    )

(defn insert_obj_to_db_fun [^Ignite ignite ^Long group_id ^String table_name ^clojure.lang.PersistentArrayMap pk_data ^clojure.lang.PersistentArrayMap dic_paras]
    (let [{pk_rs :pk_rs data_rs :data_rs} pk_data]
        (letfn [(get_binaryObject_pk [^BinaryObjectBuilder builder [f & r] ^List lst]
                    (if (some? f)
                        (let [key (format "%s_pk" (-> f :column_name)) value (my-lexical/get_jave_vs (-> f :column_type) (my-expression/plus_value_fun ignite group_id (-> f :column_type) (-> f :item_value) dic_paras))]
                            (recur (doto builder (.setField key value)) r (doto lst (.add (MyKeyValue. key value)))))
                        [builder lst]))
                (get_binaryObject [^BinaryObjectBuilder builder [f & r] ^List lst]
                    (if (some? f)
                        (let [key (-> f :column_name) value (my-lexical/get_jave_vs (-> f :column_type) (my-expression/plus_value_fun ignite group_id (-> f :column_type) (-> f :item_value) dic_paras))]
                            (recur (doto builder (.setField key value)) r (doto lst (.add (MyKeyValue. key value)))))
                        [builder lst]))
                (get_pk_rs [pk]
                    (cond (= (count pk) 1) (cond (true? (-> (first pk) :auto_increment)) (if (re-find #"^(?i)integer$|^(?i)int$" (-> (first pk) :column_type))
                                                                                             (MyConvertUtil/ConvertToInt (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                                                             (.incrementAndGet (.atomicSequence ignite (format "f_%s" table_name) 0 true)))
                                                 (false? (-> (first pk) :auto_increment)) (my-lexical/get_jave_vs (-> (first pk) :column_type) (my-expression/plus_value_fun ignite group_id (-> (first pk) :column_type) (-> (first pk) :item_value) dic_paras))
                                                 )
                          (> (count pk) 1) (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                                               (get_binaryObject_pk keyBuilder pk (ArrayList.))
                                               (throw (Exception. "表不存在主键！")))
                          :else
                          (throw (Exception. "表不存在主键！"))
                          ))
                (get_data_rs [data pk]
                    (if-let [valueBuilder (.builder (.binary ignite) (KvSql/getValueType ignite (format "f_%s" table_name)))]
                        (cond (= (count pk) 1) (get_binaryObject valueBuilder data (ArrayList.))
                              (> (count pk) 1) (get_binaryObject valueBuilder (concat (get_plus_pk pk) data) (ArrayList.))
                              :else
                              (throw (Exception. "表不存在主键！")))
                        (throw (Exception. "表不存在主键！"))))
                ]
            (let [log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true)) pk (get_pk_rs pk_rs) data (get_data_rs data_rs pk_rs)]
                (if (vector? pk)
                    [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) (.build (nth pk 0)) (.build (nth data 0)) (SqlType/INSERT))
                     (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) (nth pk 1) (nth data 1) (SqlType/INSERT)))) (SqlType/INSERT))]
                    [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) pk (.build (nth data 0)) (SqlType/INSERT))
                     (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) pk (nth data 1) (SqlType/INSERT)))) (SqlType/INSERT))])
                )
            ))
    )

; 执行 元表的sql
; insert_obj:
; {:table_name "categories",
;  :values ({:item_name "description", :item_value "'meiyy'"}
;           {:item_name "categoryname", :item_value "'wudafu'"}
;           {:item_name "categoryid", :item_value "12"})}
(defn insert_meta_table [^Ignite ignite ^clojure.lang.PersistentArrayMap insert_obj]
    (if-let [{table_name :table_name values :values} insert_obj]
        (cond (my-lexical/is-eq? table_name "my_users_group") (insert_users_group ignite insert_obj)
              (my-lexical/is-eq? table_name "my_dataset") (insert_data_set ignite insert_obj)
            )))

; 对于超级管理员执行 insert 语句
(defn insert_run_super_admin [^Ignite ignite ^String sql]
    (if-let [insert_obj (get_insert_obj sql)]
        (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case (-> insert_obj :table_name)))
            (insert_meta_table ignite insert_obj))
        ))


; 判断是否有权限
; 参数一：({:item_name "description", :item_value "'meiyy'"}
;          {:item_name "categoryname", :item_value "'wudafu'"}
;          {:item_name "categoryid", :item_value "12"})
; 参数二：{:table_name "City", :lst #{"District" "Name"}}
(defn get-authority [[f & r] view_items]
    (if-not (empty? view_items)
        (if (some? f)
            (if-not (contains? view_items (str/lower-case (:item_name f)))
                (throw (Exception. (String/format "字段 %s 没有添加的权限" (object-array [(:item_name f)]))))
                (recur r view_items)))))

; 执行需要保持 log 的 insert 语句
(defn insert_run_log [^Ignite ignite ^Long group_id ^String sql]
    (let [insert_obj (get_insert_obj sql) view_obj (get_view_obj ignite group_id sql)]
        (if (nil? (get-authority (-> insert_obj :values) (-> view_obj :lst)))
            (if-let [pk_data (get_pk_data ignite (-> insert_obj :table_name))]
                (if-let [pk_with_data (get_pk_data_with_data pk_data insert_obj)]
                    (insert_obj_to_db ignite group_id (-> insert_obj :table_name) pk_with_data)
                    )
                )
            )))

; 执行需要保持 log 的 insert 语句
(defn insert_run_log_fun [^Ignite ignite ^Long group_id ^String sql ^clojure.lang.PersistentArrayMap dic_paras]
    (let [insert_obj (get_insert_obj_fun sql dic_paras) view_obj (get_view_obj ignite group_id sql)]
        (if (nil? (get-authority (-> insert_obj :values) (-> view_obj :lst)))
            (if-let [pk_data (get_pk_data ignite (-> insert_obj :table_name))]
                (if-let [pk_with_data (get_pk_data_with_data pk_data insert_obj)]
                    (insert_obj_to_db_fun ignite group_id (-> insert_obj :table_name) pk_with_data dic_paras)
                    )
                )
            )))

; 执行不需要保持 log 的 insert 语句
(defn insert_run_no_log [^Ignite ignite ^Long group_id ^String sql]
    (let [insert_obj (get_insert_obj sql) view_obj (get_view_obj ignite group_id sql)]
        (if (nil? (get-authority (-> insert_obj :values) (-> view_obj :lst)))
            (if-let [pk_data (get_pk_data ignite (-> insert_obj :table_name))]
                (if-let [pk_with_data (get_pk_data_with_data pk_data insert_obj)]
                    (insert_obj_to_db_no_log ignite group_id (-> insert_obj :table_name) pk_with_data)
                    )
                )
            )))

; 执行不需要保持 log 的 insert 语句
(defn insert_run_no_log_fun [^Ignite ignite ^Long group_id ^String sql ^clojure.lang.PersistentArrayMap dic_paras]
    (let [insert_obj (get_insert_obj sql) view_obj (get_view_obj ignite group_id sql)]
        (if (nil? (get-authority (-> insert_obj :values) (-> view_obj :lst)))
            (if-let [pk_data (get_pk_data ignite (-> insert_obj :table_name))]
                (if-let [pk_with_data (get_pk_data_with_data pk_data insert_obj)]
                    (insert_obj_to_db_no_log_fun ignite group_id (-> insert_obj :table_name) pk_with_data dic_paras)
                    )
                )
            )))

; 1、保存参数列表 MyInputParamEx
; 2、在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是  my-lexical/get_scenes_dic
; dic_paras = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
; key 表示参数的名字，value 表示值和数据类型
(defn get_insert_cache_tran [^Ignite ignite ^Long group_id ^String sql ^clojure.lang.PersistentArrayMap dic_paras]
    (if (> group_id 0)
        (if-let [ds_obj (my-util/get_ds_by_group_id ignite group_id)]
            (if-not (empty? ds_obj)
                ; 如果是实时数据集
                (if (true? (nth (first ds_obj) 1))
                    ; 在实时数据集
                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                        (insert_run_log_fun ignite group_id sql dic_paras)
                        (insert_run_no_log_fun ignite group_id sql dic_paras)
                        )
                    ; 在非实时树集
                    (let [{table_name :table_name} (get_insert_obj sql)]
                        (if (true? (my-util/is_in_ds ignite (nth ds_obj 0) table_name))
                            (throw (Exception. "表来至实时数据集不能在该表上执行插入操作！"))
                            (insert_run_no_log_fun ignite group_id sql dic_paras)))
                    )
                (throw (Exception. "用户不存在或者没有权限！")))
            )))

; 1、保存参数列表 MyInputParamEx
; 2、在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是  my-lexical/get_scenes_dic
; dic_paras = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
; key 表示参数的名字，value 表示值和数据类型
(defn get_insert_cache [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap ast ^clojure.lang.PersistentArrayMap dic_paras]
    (if (> group_id 0)
        (if-let [ds_obj (my-util/get_ds_by_group_id ignite group_id)]
            (if-not (empty? ds_obj)
                ; 如果是实时数据集
                (if (true? (nth (first ds_obj) 1))
                    ; 在实时数据集
                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                        (insert_obj_to_db_fun ignite group_id (-> ast :table_name) ast dic_paras)
                        (insert_obj_to_db_no_log_fun ignite group_id (-> ast :table_name) ast dic_paras)
                        ;(insert_run_log_fun ignite group_id sql dic_paras)
                        ;(insert_run_no_log_fun ignite group_id sql dic_paras)
                        )
                    ; 在非实时树集
                    (if (true? (my-util/is_in_ds ignite (nth ds_obj 0) (-> ast :table_name)))
                        (throw (Exception. "表来至实时数据集不能在该表上执行插入操作！"))
                        (insert_obj_to_db_no_log_fun ignite group_id (-> ast :table_name) ast dic_paras)
                        ;(insert_run_no_log_fun ignite group_id sql dic_paras)
                        )
                    ;(let [{table_name :table_name} (get_insert_obj sql)]
                    ;    (if (true? (my-util/is_in_ds ignite (nth ds_obj 0) table_name))
                    ;        (throw (Exception. "表来至实时数据集不能在该表上执行插入操作！"))
                    ;        (insert_run_no_log_fun ignite group_id sql dic_paras)))
                    )
                (throw (Exception. "用户不存在或者没有权限！")))
            )))

; 1、判断用户组在实时数据集，还是非实时数据
; 如果是非实时数据集,
; 获取表名后，查一下，表名是否在 对应的 my_dataset_table 中，如果在就不能添加，否则直接执行 insert sql
; 2、如果是在实时数据集是否需要 log
(defn insert_run [^Ignite ignite ^Long group_id ^String sql]
    (if (= group_id 0)
        ; 超级用户
        (insert_run_super_admin ignite sql)
        ; 不是超级用户就要先看看这个用户组在哪个数据集下
        (if-let [ds_obj (my-util/get_ds_by_group_id ignite group_id)]
            (if-not (empty? ds_obj)
                ; 如果是实时数据集
                (if (true? (nth (first ds_obj) 1))
                    ; 在实时数据集
                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                        (my-lexical/trans ignite (insert_run_log ignite group_id sql))
                        (my-lexical/trans ignite (insert_run_no_log ignite group_id sql)))
                    ; 在非实时树集
                    (let [{table_name :table_name} (get_insert_obj sql)]
                        (if (true? (my-util/is_in_ds ignite (nth ds_obj 0) table_name))
                            (throw (Exception. "表来至实时数据集不能在该表上执行插入操作！"))
                            (my-lexical/trans ignite (insert_run_no_log ignite group_id sql))))
                    )
                (throw (Exception. "用户不存在或者没有权限！")))
            ))
    )

; 以下是保存到 cache 中的 scenes_name, ast, 参数列表
(defn save_scenes [^Ignite ignite ^Long group_id ^String scenes_name ^String sql_code ^clojure.lang.PersistentVector sql_lst ^String descrip ^List params ^Boolean is_batch]
    (if-let [insert_obj (get_insert_obj_lst sql_lst)]
        (if-let [pk_data (get_pk_data ignite (-> insert_obj :table_name))]
            (if-let [pk_with_data (get_pk_data_with_data pk_data insert_obj)]
                (let [m (MyScenesCache. group_id scenes_name sql_code descrip is_batch params (assoc pk_with_data :table_name (-> insert_obj :table_name)) (ScenesType/INSERT))]
                    (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
            (throw (Exception. "插入语句错误！")))
        (throw (Exception. "插入语句错误！")))
    )

; 调用
(defn call_scenes [^Ignite ignite ^Long group_id ^String scenes_name ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [vs (.get (.cache ignite "my_scenes") (str/lower-case scenes_name))]
        (if (= (.getGroup_id vs) group_id)
            (get_insert_cache ignite group_id (.getAst vs) dic_paras)
            (if-let [m_group_id (MyDbUtil/getGroupIdByCall ignite group_id scenes_name)]
                (get_insert_cache ignite m_group_id (.getAst vs) dic_paras)
                (throw (Exception. (format "用户组 %s 没有执行权限！" group_id)))))
        (throw (Exception. (format "场景名称 %s 不存在！" scenes_name)))))


(defn my_call_scenes [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap vs ^java.util.ArrayList lst_paras]
    (let [dic_paras (my-lexical/get_scenes_dic vs lst_paras)]
        (if (= (.getGroup_id vs) group_id)
            (get_insert_cache ignite group_id (.getAst vs) dic_paras)
            (if-let [m_group_id (MyDbUtil/getGroupIdByCall ignite group_id (.getScenes_name vs))]
                (get_insert_cache ignite m_group_id (.getAst vs) dic_paras)
                (throw (Exception. (format "用户组 %s 没有执行权限！" group_id))))))
    )

; 调用
(defn -my_call_scenes [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap vs ^java.util.ArrayList lst_paras]
    (my_call_scenes ignite group_id vs lst_paras)
    )










































