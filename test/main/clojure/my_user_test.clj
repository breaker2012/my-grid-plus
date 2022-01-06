(ns my-user-test
  (:require
      [clojure.spec.alpha :as s]
      [org.gridgain.plus.tools.my-cache :as my-cache]
      [org.gridgain.plus.tools.my-util :as my-util]
      [clojure.core.reducers :as r]
      [clojure.string :as str])
  (:import (org.apache.ignite Ignite IgniteCache)
           (org.apache.ignite.internal IgnitionEx)
           (org.apache.ignite.configuration CacheConfiguration)
           (org.apache.ignite.cache CacheMode)
           (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
           (org.tools MyDbUtil MyTools)
           (java.nio.file Files Paths)
           java.net.URI
           org.gridgain.dml.util.MyCacheExUtil))

; 连接数据库
(def springCfgPath "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml")
(def ignite (IgnitionEx/start springCfgPath))

(defn get-root-user [cfgPath]
    (-> cfgPath (URI.) (Paths/get) (Files/readAllBytes) (MyCacheExUtil/restore)))

; 测试
(defn login [user_name pass_word cfgPath]
  (when-let [rs (MyDbUtil/runMetaSql ignite "select m.id, m.group_id from my_user as m where m.user_name = ? and m.pass_word = ?" [user_name pass_word])]
    (if (some? rs)
      (first rs)
      (let [root_user (get-root-user cfgPath)]
        (if (some? root_user)
          (do [0 -1] (println root_user))
          nil)))))

(login "myy" "myy" "file:///Users/chenfei/temp/grid")