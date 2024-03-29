(ns org.gridgain.plus.tools.my-cache
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyCache
        ; 是否生成 class 的 main 方法
        :main false
        :state state
        ; init 构造函数
        :init init
        ; 构造函数
        :constructors {[org.apache.ignite.Ignite] []}
        ; 生成 java 调用的方法
        :methods [[getMetaCache [] org.apache.ignite.IgniteCache]
                  [getPublicCache [] org.apache.ignite.IgniteCache]]
        ))

; 构造函数
(defn -init [^Ignite ignite]
    [[] (atom {:ignite ignite})])

; 获取 meta cache
(defn -getMetaCache
    [this]
    (let [ignite (@(.state this) :ignite)]
        (.cache ignite "my_meta_table")
        ))

; 获取 public cache
(defn -getPublicCache
    [this]
    (let [ignite (@(.state this) :ignite)]
        (.cache ignite "public_meta")
        ))


