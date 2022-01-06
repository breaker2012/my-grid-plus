(ns org.gridgain.plus.dml.my-select-case-1
  (:require
    [org.gridgain.plus.dml.my-select :as my-select]
    [org.gridgain.plus.dml.select-lexical :as my-lexical]
    [clojure.core.reducers :as r]
    [clojure.string :as str]
    [clojure.walk :as w]
    [clojure.test :refer :all]))

(println (my-select/get-item-alias "m.name"))