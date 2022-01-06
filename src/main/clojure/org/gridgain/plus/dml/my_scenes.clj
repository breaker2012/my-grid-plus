(ns org.gridgain.plus.dml.my-scenes
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-cron :as my-cron]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.dml.my-scenes-util :as my-scenes-util]
        [org.gridgain.plus.dml.my-trans :as my-trans]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache MyScenesParams MyScenesParamsPk)
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
        :name org.gridgain.plus.dml.MyScenes
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 以下是保存到 cache 中的 scenes_name, ast, 参数列表
(defn save_scenes [^Ignite ignite ^Long group_id ^String scenes_code]
    (let [{scenes_type :scenes_type scenes_obj :obj} (my-scenes-util/my_scenes_obj scenes_code)]
        (cond (= scenes_type "scenes") (let [{scenes_name :name params :params sql :sql descrip :descrip is_batch :is_batch} scenes_obj]
                                           (cond (my-lexical/is-eq? (first sql) "select") (my-select/save_scenes ignite group_id scenes_name scenes_code sql descrip params is_batch)
                                                 (my-lexical/is-eq? (first sql) "insert") (my-insert/save_scenes ignite group_id scenes_name scenes_code sql descrip params is_batch)
                                                 (my-lexical/is-eq? (first sql) "update") (my-update/save_scenes ignite group_id scenes_name scenes_code sql descrip params is_batch)
                                                 (my-lexical/is-eq? (first sql) "delete") (my-delete/save_scenes ignite group_id scenes_name scenes_code sql descrip params is_batch)
                                                 )
                                           )
              (= scenes_type "tran") (let [{scenes_name :name params :params trans :trans descrip :descrip is_batch :is_batch} scenes_obj]
                                           (let [m (MyScenesCache. group_id scenes_name scenes_code descrip is_batch params (my-trans/get_trans_to_json_lst trans))]
                                               (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
              (= scenes_type "cron") (let [{scenes_name :name params :params descrip :descrip is_batch :is_batch} scenes_obj]
                                           (let [m (MyScenesCache. group_id scenes_name scenes_code descrip is_batch params (my-cron/add-job ignite group_id scenes_obj))]
                                               (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
              )))
















































































