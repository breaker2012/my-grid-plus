(ns org.gridgain.plus.ddl.my-create-table
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType DdlLog DataSetDdlLog)
             (cn.plus.model.ddl MyDataSet MyDatasetTable MyDatasetTablePK MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK MyTableObj MyUpdateViews MyViewObj ViewOperateType ViewType)
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyCreateTable
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [plus_create_table [org.apache.ignite.Ignite Long String String String] void]]
        ))

(defn get_tmp_item [^String item_line]
    (if-let [items (my-lexical/to-back item_line)]
        (if (and (= (count items) 3) (= (nth items 1) "="))
            {:my_left (nth items 0) :my_right (nth items 2)}
            (throw (Exception. (format "创建表的语句中 WITH 语句出错！位置：%s" item_line))))
        (throw (Exception. (format "创建表的语句中 WITH 语句出错！位置：%s" item_line)))))

(defn get_tmp_items
    ([ignite lst_line] (get_tmp_items ignite lst_line [] (StringBuilder.)))
    ([ignite [f & r] lst sb]
     (if (some? f)
         (if (< (count lst) 2)
             (let [{my_left :my_left my_right :my_right} (get_tmp_item f)]
                 (cond (.containsKey (.getTemplateConfiguration (.configuration ignite)) my_right) (recur ignite r (conj lst f) (doto sb (.append (format "%s," (.getTemplateValue (.get (.getTemplateConfiguration (.configuration ignite)) my_right))))))
                       (my-lexical/is-eq? my_left "AFFINITY_KEY") (recur ignite r (conj lst f) (doto sb (.append (format "AFFINITY_KEY=%s," my_right))))
                       :else
                       (throw (Exception. "创建表的语句中 WITH 语句出错！只能是 TEMPLATE=XXX,AFFINITY_KEY=YYY 这种形式"))
                       ))
             (throw (Exception. "创建表的语句中 WITH 语句出错！只能是 TEMPLATE=XXX,AFFINITY_KEY=YYY 这种形式")))
         (.toString sb))))

(defn get_tmp_line [^Ignite ignite ^String template_line]
    (if-let [line_last (re-find #"\"$" template_line)]
        (if-let [line (str/replace template_line #"\"$" "")]
            (if-let [lst_line (str/split line #"\s*,\s*")]
                (get_tmp_items ignite lst_line)
                (throw (Exception. "创建表的语句中 WITH 语句出错！")))
            (throw (Exception. "创建表的语句中 WITH 语句出错！")))
        (throw (Exception. "创建表的语句中 WITH 语句出错！"))))

; 事务执行 DDL
; 创建一个recode 记录 (sql un_sql 执行成功)
; 形成这样的列表，当执行中有 false 就执行 un_sql，
; 来回滚事务
;(defrecord ddl [^String sql ^String un_sql ^Boolean is_success])

(defn sql_lst
    ([lst] (sql_lst lst []))
    ([[f & r] rs]
     (if (some? f)
         (if (nil? r) (recur r (concat rs [f]))
                      (recur r (concat rs [f " "])))
         rs)))

(defn get_sql [^String sql]
    (str/join (sql_lst (my-lexical/to-back sql))))

; 获取 items 和 template
(defn get_items_tp [^String line_rs]
    (if-let [items (str/split line_rs #"(?i)\s\)\sWITH\s\"")]
        (if (= (count items) 2)
            {:items_line (get items 0) :template (get items 1)})
        (throw (Exception. "创建表的语句错误！没有 with 关键词！"))))

(defn get_items
    ([lst] (get_items lst [] [] []))
    ([[f & r] lst_stack item_stack lst]
     (if (some? f)
         (cond (= f "(") (recur r (conj lst_stack f) (conj item_stack f) lst)
               (= f ")") (recur r (pop lst_stack) (conj item_stack f) lst)
               (and (= f ",") (= (count lst_stack) 0) (> (count item_stack) 0)) (recur r lst_stack [] (conj lst item_stack))
               :else
               (recur r lst_stack (conj item_stack f) lst)
               )
         (if (> (count item_stack) 0)
             (conj lst item_stack)
             lst))))

(defn get_item_obj
    ([lst] (get_item_obj lst [] [] [] []))
    ([[f & r] pk_stack type_stack lst_type lst]
     (if (some? f)
         (cond (and (my-lexical/is-eq? f "comment") (= (first r) "(") (= (second (rest r)) ")")) (recur (rest (rest (rest r))) pk_stack type_stack lst_type (conj lst {:comment (my-lexical/get_str_value (second r))}))
               (and (my-lexical/is-eq? f "PRIMARY") (my-lexical/is-eq? (first r) "KEY")) (if (> (count (rest r)) 0) (recur (rest (rest r)) (conj pk_stack (second r)) type_stack lst_type lst)
                                                                                                    (recur nil nil type_stack lst_type (conj lst {:pk [(first lst)]})))
               (and (> (count pk_stack) 0) (= f ")")) (recur r [] type_stack lst_type (conj lst {:pk (filter #(not= % ",") (rest pk_stack))}))
               (and (> (count pk_stack) 0) (not= f ")")) (recur r (conj pk_stack f) type_stack lst_type lst)
               (and (> (count type_stack) 0) (= f "(")) (recur r pk_stack (conj type_stack f) (conj lst_type f) lst)
               (and (> (count type_stack) 0) (not= f ")")) (recur r pk_stack type_stack (conj lst_type f) lst)
               (and (> (count type_stack) 0) (= f ")")) (if (= (count type_stack) 1) (recur r pk_stack [] [] (conj (pop lst) (assoc (peek lst) :vs lst_type))))
               (and (my-lexical/is-eq? f "NOT") (my-lexical/is-eq? (first r) "NULL") (= (count pk_stack) 0)) (recur (rest r) pk_stack type_stack lst_type (conj lst {:not_null true}))
               (my-lexical/is-eq? f "auto") (recur r pk_stack type_stack lst_type (conj lst {:auto true}))
               (some? (re-find #"^(?i)float$|^(?i)double$|^(?i)long$|^(?i)integer$|^(?i)int$|^(?i)SMALLINT$|^(?i)TINYINT$|^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$|^(?i)BOOLEAN$|^(?i)BIGINT$|^(?i)BINARY$|^(?i)TIMESTAMP$|^(?i)Date$|^(?i)TIME$|^(?i)DECIMAL$|^(?i)REAL$" f)) (if (= (first r) "(")
                                                                                                                                                                                                                                                                                                             (recur (rest r) pk_stack (conj type_stack (first r)) lst_type (conj lst {:type (my-lexical/convert_to_type f)}))
                                                                                                                                                                                                                                                                                                             (recur r pk_stack [] lst_type (conj lst {:type (my-lexical/convert_to_type f)})))
               (and (my-lexical/is-eq? f "DEFAULT") (some? (first r))) (recur (rest r) pk_stack type_stack lst_type (conj lst {:default (first r)}))
               :else
               (recur r pk_stack type_stack lst_type (conj lst f)))
         lst)))

(defn items_obj [my_items]
    (loop [[f & r] my_items lst_items []]
        (if (some? f)
            (recur r (conj lst_items (get_item_obj f)))
            lst_items)))

(defn to_item
    ([lst] (to_item lst (MyTableItem.) (StringBuilder.) #{}))
    ([[f & r] ^MyTableItem m ^StringBuilder code_line pk_set]
     (if (some? f)
         (cond (and (instance? String f) (= (Strings/isNullOrEmpty (.getColumn_name m)) true)) (let [column_name (str/lower-case f)]
                                                                                                   (.setColumn_name m column_name)
                                                                                                   (.append code_line column_name)
                                                                                                   (recur r m code_line pk_set))
               (and (instance? String f) (= (Strings/isNullOrEmpty (.getColumn_name m)) false)) (throw (Exception. (format "语句错误，位置在：%s" f)))
               (and (map? f) (contains? f :pk) (= (Strings/isNullOrEmpty (.getColumn_name m)) false)) (if (= (count pk_set) 0)
                                                                                                          (do
                                                                                                              (.setPkid m true)
                                                                                                              (recur r m code_line (conj pk_set (.getColumn_name m))))
                                                                                                          (throw (Exception. "组合主键设置错误！")))
               (and (map? f) (contains? f :pk) (= (Strings/isNullOrEmpty (.getColumn_name m)) true)) (recur r m code_line (concat pk_set (-> f :pk)))
               (and (map? f) (contains? f :type)) (do (.setColumn_type m (-> f :type))
                                                      (.append code_line " ")
                                                      (.append code_line (-> f :type))
                                                      (if (contains? f :vs)
                                                          (cond (= (count (-> f :vs)) 1) (let [len (nth (-> f :vs) 0)]
                                                                                             (.setColumn_len m (MyConvertUtil/ConvertToInt len))
                                                                                             (.append code_line "(")
                                                                                             (.append code_line len)
                                                                                             (.append code_line ")"))
                                                                (= (count (-> f :vs)) 3) (let [len (nth (-> f :vs) 0) scale (nth (-> f :vs) 2)]
                                                                                             (.setColumn_len m (MyConvertUtil/ConvertToInt len))
                                                                                             (.setScale m (MyConvertUtil/ConvertToInt scale))
                                                                                             (.append code_line "(")
                                                                                             (.append code_line len)
                                                                                             (.append code_line ",")
                                                                                             (.append code_line scale)
                                                                                             (.append code_line ")"))
                                                                ))
                                                      (recur r m code_line pk_set))
               (and (map? f) (contains? f :not_null)) (do (.setNot_null m (-> f :not_null))
                                                          (.append code_line " not null")
                                                          (recur r m code_line pk_set))
               (and (map? f) (contains? f :default)) (do (.setDefault_value m (-> f :default))
                                                         (.append code_line (.concat " DEFAULT " (-> f :default)))
                                                         (recur r m code_line pk_set))
               (and (map? f) (contains? f :comment)) (do (.setComment m (-> f :comment))
                                                         (recur r m code_line pk_set))
               (and (map? f) (contains? f :auto)) (do (.setAuto_increment m (-> f :auto))
                                                      (recur r m code_line pk_set))
               )
         {:table_item m :code code_line :pk pk_set})))

(defn set_pk
    ([lst_table_item column_name] (set_pk lst_table_item column_name []))
    ([[f & r] ^String column_name ^ArrayList lst]
     (if (some? f)
         (if (my-lexical/is-eq? (.getColumn_name f) column_name)
             (recur r column_name (conj lst (doto f (.setPkid true))))
             (recur r column_name (conj lst f)))
         lst)))

(defn set_pk_set [^ArrayList lst_table_item [f & r]]
    (if (some? f)
        (recur (set_pk lst_table_item f) r)
        lst_table_item))

(defn item_obj [[f & r] item_name]
    (if (and (some? f) (my-lexical/is-eq? (.getColumn_name f) item_name))
        f
        (recur r item_name)))

(defn new_pk [[f & r] lst_table_item ^StringBuilder sb]
    (if (some? f)
        (if-let [m (item_obj lst_table_item f)]
            (do
                (.append sb (.concat (.getColumn_name m) "_pk"))
                (.append sb (.concat " " (.getColumn_type m)))
                (cond (and (not (nil? (.getColumn_len m))) (not (nil? (.getScale m))) (> (.getColumn_len m) 0) (> (.getScale m) 0)) (.append sb (str/join ["(" (.getColumn_len m) "," (.getScale m) ")"]))
                      (and (not (nil? (.getColumn_len m))) (> (.getColumn_len m) 0) ) (.append sb (str/join ["(" (.getColumn_len m) ")"]))
                      )
                (if (= (Strings/isNullOrEmpty (.getDefault_value m)) false)
                    (.append sb (.concat " default " (.getDefault_value m))))
                (.append sb ",")
                (recur r lst_table_item sb))
            (throw (Exception. "创建表的语句中主键错误！")))
        (.toString sb)))

(defn pk_line
    ([pk_sets] (pk_line pk_sets (StringBuilder.)))
    ([[f & r] ^StringBuilder sb]
     (if (some? f)
         (if (= (count r) 0) (recur r (doto sb (.append (.concat (str/lower-case f) "_pk"))))
                             (recur r (doto sb (.append (.concat (str/lower-case f) "_pk,")))))
         (.toString sb))))

(defn get_pk_name_vs [pk_items]
    (loop [[f & r] pk_items name (StringBuilder.) value (StringBuilder.)]
        (if (some? f)
            (if (= (count r) 0)
                (recur r (doto name (.append (str/lower-case f))) (doto value (.append (str/lower-case f))))
                (recur r (doto name (.append (.concat (str/lower-case f) "_"))) (doto value (.append (.concat (str/lower-case f) ",")))))
            {:name (.toString name) :value (.toString value)})))

(defn get_pk_index_no_ds
    ([pk_sets ^String table_name] (if-let [lst_rs (get_pk_index_no_ds pk_sets table_name [])]
                                      (if-let [{name :name value :value} (get_pk_name_vs pk_sets)]
                                          (conj lst_rs {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s)" table_name name table_name (str/lower-case value)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_idx" table_name name) :is_success nil})
                                          (throw (Exception. "创建表的语句错误！")))
                                      (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^String table_name lst]
     (if (some? f)
         (recur r table_name (conj lst {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s)" table_name f table_name (str/lower-case f)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_idx" table_name f) :is_success nil}))
         lst)))

(defn get_pk_index_ds
    ([pk_sets ^String table_name ^String data_set_name] (if-let [lst_rs (get_pk_index_ds pk_sets table_name [] data_set_name)]
                                                      (if-let [{name :name value :value} (get_pk_name_vs pk_sets)]
                                                          (conj lst_rs {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_%s_idx ON %s_%s (%s)" data_set_name table_name name data_set_name table_name (str/lower-case value)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_%s_idx" data_set_name table_name name) :is_success nil})
                                                          (throw (Exception. "创建表的语句错误！")))
                                                      (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^String table_name lst ^String data_set_name]
     (if (some? f)
         (recur r table_name (conj lst {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_%s_idx ON %s_%s (%s)" data_set_name table_name f data_set_name table_name (str/lower-case f)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_%s_idx" data_set_name table_name f) :is_success nil}) data_set_name)
         lst)))

(defn get_pk_index [pk_sets ^String table_name ^String data_set_name]
    (if (not (Strings/isNullOrEmpty data_set_name))
        (get_pk_index_ds pk_sets table_name data_set_name)
        (get_pk_index_no_ds pk_sets table_name)))

(defn table_items
    ([lst_table_item] (table_items lst_table_item []))
    ([[f & r] lst]
     (if (some? f)
         (if (not (Strings/isNullOrEmpty (.getColumn_name f)))
             (recur r (conj lst f))
             (recur r lst))
         lst)))

(defn get_obj_ds
    ([items ^String table_name data_set_name] (if-let [{lst_table_item :lst_table_item code_sb :code_sb pk_sets :pk_sets} (get_obj_ds items (ArrayList.) (StringBuilder.) #{} data_set_name)]
                                                  (cond (= (count pk_sets) 1) {:lst_table_item (set_pk_set lst_table_item pk_sets) :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (nth pk_sets 0))}
                                                        (> (count pk_sets) 1) (if-let [pk_set (set_pk_set lst_table_item pk_sets)]
                                                                                  {:lst_table_item pk_set :code_sb (format "%s %s PRIMARY KEY (%s)" (.toString code_sb) (new_pk pk_sets pk_set (StringBuilder.)) (pk_line pk_sets))
                                                                                   :indexs (get_pk_index pk_sets table_name data_set_name)}
                                                                                  (throw (Exception. "主键设置错误！")))
                                                        :else
                                                        (throw (Exception. "主键设置错误！"))
                                                        )
                                                  (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^ArrayList lst ^StringBuilder sb pk_sets data_set_name]
     (if (some? f)
         (if-let [{table_item :table_item code_line :code pk :pk} (to_item f)]
             (do (.add lst table_item)
                 (if (not (nil? (last (.trim (.toString code_line)))))
                     (.append sb (.concat (.trim (.toString code_line)) ",")))
                 (recur r lst sb (concat pk_sets pk) data_set_name))
             (throw (Exception. "创建表的语句错误！")))
         {:lst_table_item (table_items lst) :code_sb sb :pk_sets pk_sets})))

(defn get_obj_no_ds
    ([items ^String table_name] (if-let [{lst_table_item :lst_table_item code_sb :code_sb pk_sets :pk_sets} (get_obj_no_ds items (ArrayList.) (StringBuilder.) #{})]
                                                    (cond (= (count pk_sets) 1) {:lst_table_item (set_pk_set lst_table_item pk_sets) :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (nth pk_sets 0))}
                                                          (> (count pk_sets) 1) (if-let [pk_set (set_pk_set lst_table_item pk_sets)]
                                                                                    {:lst_table_item pk_set :code_sb (format "%s %s PRIMARY KEY (%s)" (.toString code_sb) (new_pk pk_sets pk_set (StringBuilder.)) (pk_line pk_sets))
                                                                                     :indexs (get_pk_index pk_sets table_name nil)}
                                                                                    (throw (Exception. "主键设置错误！")))
                                                          :else
                                                          (throw (Exception. "主键设置错误！"))
                                                          )
                                                    (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^ArrayList lst ^StringBuilder sb pk_sets]
     (if (some? f)
         (if-let [{table_item :table_item code_line :code pk :pk} (to_item f)]
             (do (.add lst table_item)
                 (if (not (nil? (last (.trim (.toString code_line)))))
                     (.append sb (.concat (.trim (.toString code_line)) ",")))
                 (recur r lst sb (concat pk_sets pk)))
             (throw (Exception. "创建表的语句错误！")))
         {:lst_table_item (table_items lst) :code_sb sb :pk_sets pk_sets})))

(defn get_obj [items ^String table_name data_set_name]
    (if (and (some? data_set_name) (not (Strings/isNullOrEmpty (first (first data_set_name)))))
        (get_obj_ds items table_name (first (first data_set_name)))
        (get_obj_no_ds items table_name)))

; items: "( CategoryID INTEGER NOT NULL auto comment ( '产品类型ID' ) , CategoryName VARCHAR ( 15 ) NOT NULL comment ( '产品类型名' ) , Description VARCHAR comment ( '类型说明' ) , Picture VARCHAR comment ( '产品样本' ) , PRIMARY KEY ( CategoryID )"
; items: "( id int PRIMARY KEY , city_id int , name varchar , age int , company varchar"
(defn get_items_obj [items]
    (when-let [my_items (items_obj (get_items (rest (my-lexical/to-back items))))]
        my_items))

(defn get_template [^Ignite ignite ^String table_name data_set_name ^String template]
    (if (and (some? data_set_name) (= (count data_set_name) 1) (not (Strings/isNullOrEmpty (first (first data_set_name)))))
        (format "%scache_name=f_%s_%s\"" (get_tmp_line ignite template) (first (first data_set_name)) table_name)
        (format "%scache_name=f_%s\"" (get_tmp_line ignite template) table_name)))

; 输入 sql_line 返回 {:create_table "" :table_name "" :line_rs ""}
(defn get_table_line_obj [^Ignite ignite ^String sql_line & data_set_name]
    (if-let [sql (get_sql sql_line)]
        (if-let [sql1 (re-find #"^(?i)CREATE\sTABLE\sIF\sNOT\sEXISTS\s\w+\s\(" sql)]
            (if-let [{items_line :items_line template :template} (get_items_tp (str/replace sql #"^(?i)CREATE\sTABLE\sIF\sNOT\sEXISTS\s\w+\s\(" "("))]
                (let [table_name (str/replace (str/replace sql1 #"^(?i)CREATE\sTABLE\sIF\sNOT\sEXISTS\s" "") #"\s\($" "") items (get_items_obj items_line)]
                    (if-let [{lst_table_item :lst_table_item code_sb :code_sb indexs :indexs} (get_obj items table_name data_set_name)]
                        {:create_table "CREATE TABLE IF NOT EXISTS"
                         :table_name table_name
                         :lst_table_item lst_table_item
                         :code_sb (.toString code_sb)
                         :indexs indexs
                         :template (get_template ignite table_name data_set_name template)
                         }
                        (throw (Exception. "创建表的语句错误！"))))
                (throw (Exception. "创建表的语句错误！没有 with 关键词！")))
            (if-let [sql2 (re-find #"^(?i)CREATE\sTABLE\s\w+\s\(" sql)]
                (if-let [{items_line :items_line template :template} (get_items_tp (str/replace sql #"^(?i)CREATE\sTABLE\s\w+\s\(" "("))]
                    (let [table_name (str/replace (str/replace sql2 #"^(?i)CREATE\sTABLE\s" "") #"\s\($" "") items (get_items_obj items_line)]
                        (if-let [{lst_table_item :lst_table_item code_sb :code_sb indexs :indexs} (get_obj items table_name data_set_name)]
                            {:create_table "CREATE TABLE"
                             :table_name table_name
                             :lst_table_item lst_table_item
                             :code_sb (.toString code_sb)
                             :indexs indexs
                             :template (get_template ignite table_name data_set_name template)
                             }
                            (throw (Exception. "创建表的语句错误！"))))
                    (throw (Exception. "创建表的语句错误！没有 with 关键词！")))
                ))
        (throw (Exception. "创建表的语句错误！"))))

; json 转换为 ddl 序列
(defn to_ddl_lst [^Ignite ignite ^String sql_line & data_set_name]
    (if-let [{create_table :create_table table_name :table_name lst_table_item :lst_table_item code_sb :code_sb indexs :indexs template :template} (get_table_line_obj ignite sql_line data_set_name)]
        (if (and (some? data_set_name) (not (Strings/isNullOrEmpty (first data_set_name))))
            {:table_name table_name :lst_table_item lst_table_item :lst_ddl (concat (conj [] {:sql (format "%s %s_%s (%s) WITH \"%s" create_table (first data_set_name) table_name code_sb template) :un_sql (format "DROP TABLE IF EXISTS %s_%s" (first data_set_name) table_name) :is_success nil}) indexs)}
            {:table_name table_name :lst_table_item lst_table_item :lst_ddl (concat (conj [] {:sql (format "%s %s (%s) WITH \"%s" create_table table_name code_sb template) :un_sql (format "DROP TABLE IF EXISTS %s" table_name) :is_success nil}) indexs)})
        (throw (Exception. "创建表的语句错误！"))))

; 生成 my_meta_tables
(defn get_table_obj [^Ignite ignite ^String table_name ^String descrip ^String code ^Long data_set_id]
    (if-let [id (.incrementAndGet (.atomicSequence ignite "my_meta_tables" 0 true))]
        (MyTable. id table_name descrip code data_set_id)
        (throw (Exception. "数据库异常！"))))

; 生成 MyTable
(defn get_table_items_obj
    ([^Ignite ignite lst_table_item table_id] (get_table_items_obj ignite lst_table_item table_id []))
    ([^Ignite ignite [f & r] table_id lst]
     (if (some? f)
         (if-let [id (.incrementAndGet (.atomicSequence ignite "table_item" 0 true))]
             (recur ignite r table_id (conj lst {:table "table_item" :key (MyTableItemPK. id table_id) :value (doto f (.setId id)
                                                                                                  (.setTable_id table_id))}))
             (throw (Exception. "数据库异常！")))
         lst)))

; 生成 MyCacheEx
(defn get_my_table [^Ignite ignite ^String table_name ^String descrip ^String code lst_table_item ^Long data_set_id]
    (if-let [table (get_table_obj ignite table_name descrip code data_set_id)]
        (if-let [lst_items (get_table_items_obj ignite lst_table_item (.getId table))]
            (cons {:table "my_meta_tables" :key (.getId table) :value table} lst_items)
            (throw (Exception. "数据库异常！")))
        (throw (Exception. "数据库异常！"))))

(defn to_mycachex
    ([^Ignite ignite lst_dml_table] (to_mycachex ignite lst_dml_table (ArrayList.)))
    ([^Ignite ignite [f & r] lst]
     (if (some? f)
         (recur ignite r (doto lst (.add (MyCacheEx. (.cache ignite (-> f :table)) (-> f :key) (-> f :value) (SqlType/INSERT)))))
         lst)))

; 先执行 lst_ddl 全部成功后，在执行 lst_dml_table
; 如果 lst_dml_table 执行失败，上面的也要回滚
; lst_ddl： [ddl]
; lst_dml_table: [{:table "表名" :key PK_ID :value 值}]
; 转成 ArrayList 用 java 来执行
(defn run_ddl_dml [^Ignite ignite lst_ddl lst_dml_table]
    (MyCreateTableUtil/run_ddl_dml ignite (my-lexical/to_arryList lst_ddl) lst_dml_table))

(defn create-table [^Ignite ignite ^Long group_id ^String code]
    (let [sql_line (str/lower-case code) descrip ""]
        (if (= group_id 0)
            (if-let [{table_name :table_name lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lst ignite sql_line)]
                (if-let [lst_dml_table (to_mycachex ignite (get_my_table ignite table_name descrip sql_line lst_table_item 0))]
                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                        (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                            (run_ddl_dml ignite lst_ddl (doto lst_dml_table (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id code 0) (SqlType/INSERT))))))
                        (run_ddl_dml ignite lst_ddl lst_dml_table))
                    (throw (Exception. "创建表的语句错误！")))
                (throw (Exception. "创建表的语句错误！")))
            (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
                (let [group_type (.getGroup_type my_group) dataset (.get (.cache ignite "my_dataset") (.getData_set_id my_group))]
                    (if (contains? #{"ALL" "DDL"} group_type)
                        (if (and (some? dataset) (false? (.getIs_real dataset)))
                            (if-let [{table_name :table_name lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lst ignite sql_line (.getDataset_name dataset))]
                                (if-let [lst_dml_table (to_mycachex ignite (get_my_table ignite table_name descrip sql_line lst_table_item (.getId dataset)))]
                                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                                        (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                                            (run_ddl_dml ignite lst_ddl (doto lst_dml_table (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id code (.getData_set_id my_group)) (SqlType/INSERT))))))
                                        (run_ddl_dml ignite lst_ddl lst_dml_table))
                                    (throw (Exception. "创建表的语句错误！")))
                                (throw (Exception. "创建表的语句错误！")))
                            (if-let [{table_name :table_name lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lst ignite sql_line)]
                                (if-let [lst_dml_table (to_mycachex ignite (get_my_table ignite table_name descrip sql_line lst_table_item 0))]
                                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                                        (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                                            (run_ddl_dml ignite lst_ddl (doto lst_dml_table (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id code (.getData_set_id my_group)) (SqlType/INSERT))))))
                                        (run_ddl_dml ignite lst_ddl lst_dml_table))
                                    (throw (Exception. "创建表的语句错误！")))
                                (throw (Exception. "创建表的语句错误！"))))
                        (throw (Exception. "该用户组没有创建表的权限！"))))
                (throw (Exception. "不存在该用户组！"))))))

; 执行 json
; 1、查询 group_id 看是否有 ddl 的权限
; 2、有权限就执行 sql_line
(defn my_create_table [^Ignite ignite ^Long group_id ^String table_name ^String descrip ^String code]
    (let [sql_line (str/lower-case code)]
        (if (= group_id 0)
            (if-let [{lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lst ignite sql_line)]
                (if-let [lst_dml_table (to_mycachex ignite (get_my_table ignite table_name descrip sql_line lst_table_item 0))]
                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                        (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                            (run_ddl_dml ignite lst_ddl (doto lst_dml_table (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id code 0) (SqlType/INSERT))))))
                        (run_ddl_dml ignite lst_ddl lst_dml_table))
                    (throw (Exception. "创建表的语句错误！")))
                (throw (Exception. "创建表的语句错误！")))
            (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
                (let [group_type (.getGroup_type my_group) dataset (.get (.cache ignite "my_dataset") (.getData_set_id my_group))]
                    (if (contains? #{"ALL" "DDL"} group_type)
                        (if (and (some? dataset) (false? (.getIs_real dataset)))
                            (if-let [{lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lst ignite sql_line (.getDataset_name dataset))]
                                (if-let [lst_dml_table (to_mycachex ignite (get_my_table ignite table_name descrip sql_line lst_table_item (.getId dataset)))]
                                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                                        (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                                            (run_ddl_dml ignite lst_ddl (doto lst_dml_table (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id code (.getData_set_id my_group)) (SqlType/INSERT))))))
                                        (run_ddl_dml ignite lst_ddl lst_dml_table))
                                    (throw (Exception. "创建表的语句错误！")))
                                (throw (Exception. "创建表的语句错误！")))
                            (if-let [{lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lst ignite sql_line)]
                                (if-let [lst_dml_table (to_mycachex ignite (get_my_table ignite table_name descrip sql_line lst_table_item 0))]
                                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                                        (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                                            (run_ddl_dml ignite lst_ddl (doto lst_dml_table (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id code (.getData_set_id my_group)) (SqlType/INSERT))))))
                                        (run_ddl_dml ignite lst_ddl lst_dml_table))
                                    (throw (Exception. "创建表的语句错误！")))
                                (throw (Exception. "创建表的语句错误！"))))
                        (throw (Exception. "该用户组没有创建表的权限！"))))
                (throw (Exception. "不存在该用户组！")))))
    )

; java 中调用
(defn -plus_create_table [^Ignite ignite ^Long group_id ^String table_name ^String descrip ^String code]
    (my_create_table ignite group_id table_name descrip code))







































