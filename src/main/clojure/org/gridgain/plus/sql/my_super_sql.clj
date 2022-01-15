(ns org.gridgain.plus.sql.my-super-sql
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-alter-dataset :as my-alter-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.ddl.my-update-dataset :as my-update-dataset]
        [org.gridgain.plus.dml.my-scenes :as my-scenes]
        [org.gridgain.plus.dml.my-trans :as my-trans]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.sql.MySuperSql
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [superSql [org.apache.ignite.Ignite String String] String]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 通过 userToken 获取 group_id
(defn get_group_id [^Ignite ignite ^String userToken]
    ;(if (= userToken ))
    (when-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select g.id from my_users_group as g where g.userToken = ?") (to-array [userToken])))))]
        (first m)))

(def my_group_id (memoize get_group_id))

; 是否 select 语句
(defn has-from? [[f & r]]
    (if (some? f)
        (if (my-lexical/is-eq? f "from")
            true
            (recur r))
        false))

(defn is-scenes? [lst]
    (if (and (my-lexical/is-eq? (first lst) "scenes") (= (second lst) "(") (= (last lst) ")"))
        true
        false))

(defn get-scenes [lst]
    (loop [index 2 my-count (count lst) rs []]
        (if (< (+ index 1) my-count)
            (recur (+ index 1) my-count (conj rs (nth lst index)))
            (my-lexical/get_str_value (str/join " " rs)))))

; 输入 group_id, sql 转换为，可执行的 sql
(defn super-sql [^Ignite ignite ^String userToken ^String sql]
    (if-not (Strings/isNullOrEmpty sql)
        (let [lst (my-lexical/to-back sql) group_id (my_group_id ignite userToken)]
            (cond (my-lexical/is-eq? (first lst) "insert") (let [rs (my-insert/insert_run ignite group_id sql)]
                                                                   (if (nil? rs)
                                                                       (format "select showMsg('插入语句执行成功！')")))
                  (my-lexical/is-eq? (first lst) "update") (let [rs (my-update/update_run ignite group_id sql)]
                                                                   (if (nil? rs)
                                                                       (format "select showMsg('更新语句执行成功！')")))
                  (my-lexical/is-eq? (first lst) "delete") (let [rs (my-delete/delete_run ignite group_id sql)]
                                                                   (if (nil? rs)
                                                                       (format "select showMsg('删除语句执行成功！')")))
                  (my-lexical/is-eq? (first lst) "select") (if (has-from? (rest lst))
                                                                   (my-select/get_my_ast ignite group_id sql)
                                                                   sql)
                  ; 执行事务
                  (and (= (first lst) "{") (= (last lst) "}")) (my-trans/tran_run ignite group_id sql)

                  ; 保存 scenes
                  (is-scenes? lst) (my-scenes/save_scenes ignite group_id (get-scenes lst))

                  ; ddl
                  ; create dataset
                  (and (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "dataset")) (my-create-dataset/create_data_set ignite group_id sql)
                  ; alert dataset
                  (and (my-lexical/is-eq? (first lst) "ALTER") (my-lexical/is-eq? (second lst) "dataset")) (my-alter-dataset/alter_data_set ignite group_id sql)
                  ; drop dataset
                  (and (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "dataset")) (my-drop-dataset/drop_data_set ignite group_id sql)
                  ; create table
                  (and (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "table")) (my-create-table/create-table ignite group_id sql)
                  ; alter table
                  (and (my-lexical/is-eq? (first lst) "ALTER") (my-lexical/is-eq? (second lst) "table")) (my-alter-table/my_alter_table ignite group_id sql)
                  ; drop table
                  (and (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "table")) (my-drop-table/drop_table ignite group_id sql)
                  ; create index
                  (and (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "INDEX")) (my-create-index/create_index ignite group_id sql)
                  ; drop index
                  (and (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "INDEX")) (my-drop-index/drop_index ignite group_id sql)
                  ; create table
                  (and (my-lexical/is-eq? (first lst) "update") (my-lexical/is-eq? (second lst) "dataset")) (my-update-dataset/update_dataset ignite group_id sql)
                  ))))

(defn -superSql [^Ignite ignite ^String userToken ^String sql]
    (super-sql ignite userToken sql))










































































