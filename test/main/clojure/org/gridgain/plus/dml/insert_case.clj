(ns org.gridgain.plus.dml.insert-case
  (:require
    [org.gridgain.plus.dml.select-lexical :as my-lexical]
    [org.gridgain.plus.dml.my-select :as my-select]
    [org.gridgain.plus.dml.my-insert :as my-insert]
    [org.gridgain.plus.dml.my-update :as my-update]
    [org.gridgain.plus.ddl.my-create-table :as my-create-table]
    [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
    [org.gridgain.plus.dml.my-expression :as my-expression]
    [org.gridgain.plus.context.my-context :as my-context]
    [clojure.core.reducers :as r]
    [clojure.string :as str]
    [clojure.test :refer :all])
  (:import (org.apache.ignite Ignite IgniteCache)
           (org.apache.ignite.internal IgnitionEx)
           (com.google.common.base Strings)
           (org.tools MyConvertUtil MyTools)
           (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType DdlLog DataSetDdlLog)
           (cn.plus.model.ddl MyDataSet MyDatasetTable MyDatasetRealTable MyDatasetTablePK MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK MyTableObj MyUpdateViews MyViewObj ViewOperateType ViewType)
           (cn.plus.tools KvSql)
           (org.gridgain.dml.util MyCacheExUtil)
           (org.apache.ignite.configuration CacheConfiguration)
           (org.apache.ignite.cache CacheMode CacheAtomicityMode)
           (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
           (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
           (org.gridgain.meta.cache MyContextCacheUtil)
           (org.gridgain.mydml MyTransUtil)
           (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
           (java.util ArrayList Date Iterator)
           (java.sql Timestamp)
           (java.math BigDecimal)
           (cn.plus.model.scene MySences MyInputParam MyInputParamEx)))

(def sql_lines "INSERT INTO Categories VALUES(2,'Condiments','Sweet and savory sauces, relishes, spreads, and seasonings', '');\nINSERT INTO Categories VALUES(3,'Confections','Desserts, candies, and sweet breads', '');\nINSERT INTO Categories VALUES(4,'Dairy Products','Cheeses', '');\nINSERT INTO Categories VALUES(5,'Grains/Cereals','Breads, crackers, pasta, and cereal', '');\nINSERT INTO Categories VALUES(6,'Meat/Poultry','Prepared meats', '');\nINSERT INTO Categories VALUES(7,'Produce','Dried fruit and bean curd', '');\nINSERT INTO Categories VALUES(8,'Seafood','Seaweed and fish', '');")

; 获取 code 序列
(defn get-code-lst
    [code]
    (str/split (MyTools/eliminate_comment code) #";"))

; 执行 sql 序列
(defn run_log_sql [ignite [f & r] group_id]
    (if (some? f)
        (do
            (my-insert/insert_run_log ignite group_id f)
            (recur ignite r group_id))))




































































