(ns my-core-test
  (:require [clojure.test :refer :all])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.tools MyDbUtil MyTools)
             (java.nio.file Files Paths)
             java.net.URI
             org.gridgain.dml.util.MyCacheExUtil))

; 启动 repl
;(def springCfgPath "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml")
;(def ignite (IgnitionEx/start springCfgPath))

(def m {:name "吴大富" :age 100})
(def dog {:name "美羊羊" :fird [m 1 2 "吴大贵"]})
(println (MyTools/toString (MyTools/toHashtable dog)))

; item 定义
(defrecord select-item [^String table_alias ^String item_name ^String item_type ^String java_item_type ^Boolean const])

(def m (select-item. "m" "吴大富" "" "" false))
(update m :item_name "吴大贵")
(println (-> m :item_name))
(println (:table_alias m))