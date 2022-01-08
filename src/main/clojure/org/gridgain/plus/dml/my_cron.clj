(ns org.gridgain.plus.dml.my-cron
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.dml.my-scenes-util :as my-scenes-util]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil MyPlusUtil MyPlusFunc KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache MCron SqlType)
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
        :name org.gridgain.plus.dml.MyCron
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn cron-to-str
    ([lst] (cron-to-str lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (= "*" (first r))
             (recur r (concat lst [f " "]))
             (recur r (concat lst [f])))
         (str/join lst))))

(defn run-map [ignite group_id [f & r]]
    (if (some? f)
        (do
            ;(my-expression/func_eval ignite (my-select-plus/sql-to-ast f))
            (let [func_obj (my-select-plus/sql-to-ast f)]
                (MyPlusFunc/myInvoke (-> func_obj :func-name) (to-array (cons group_id (my-expression/func_lst_ps_eval ignite group_id (-> func_obj :lst_ps))))))
            (recur ignite group_id r))))

; 批处理任务
; 输入 ast:
;{:name "名字",
; :params [#object[cn.plus.model.db.MyScenesParams 0x349f4610 "cn.plus.model.db.MyScenesParams@349f4610"]],
; :batch {:map (["f" "(" "a" ")"] ["g" "(" "b" ")"]), :reduce ("f1" "(" "a" "," "b" ")")},
; :descrip ("'描述'"),
; :cron ("{" "1" "," "3" "}" "*" "*" "*" "*" "*")}

; 添加批处理任务
(defn add-job [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap ast]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (if (.containsKey scheduledFutures (-> ast :name))
                (throw (Exception. (format "已存在任务 %s 不能添加相同的任务名！" (-> ast :name))))
                (try
                    (if-let [schedulerFuture (.scheduleLocal (.scheduler ignite) (-> ast :name) (proxy [Object Runnable] []
                                                                                                    (run []
                                                                                                        (do
                                                                                                            (run-map ignite group_id (-> (-> (-> ast :obj) :batch) :map))
                                                                                                            (run-map ignite group_id [(-> (-> (-> ast :obj) :batch) :reduce)])))) (cron-to-str (-> ast :cron)))]
                        (.put (.cache ignite "my_cron") (-> ast :name) (MCron. (-> ast :name) (cron-to-str (-> ast :cron)) (my-lexical/get_str_value (first (-> ast :descrip))) ast)))
                    (catch Exception ex
                        (.remove scheduledFutures (-> ast :name)))
                    )
                )))
    )

; 删除批处理任务
(defn remove-job [^Ignite ignite ^Long group_id ^String name]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (if (.containsKey scheduledFutures name)
                (try
                    (.remove scheduledFutures name)
                    (.remove (.cache ignite "my_cron") name)
                    (catch Exception ex
                        (add-job ignite group_id (.get (.cache ignite "my_cron") name)))
                    )
                (throw (Exception. (format "任务 %s 不存在！" name)))))))












































