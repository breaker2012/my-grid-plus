(ns my_record
  (:require
    [clojure.test :refer :all]))

(defrecord insert_kv [^String item_name iten_value])

(def m (->insert_kv "name" "吴大富"))

(println (instance? insert_kv m))
;(println (:item_name m))