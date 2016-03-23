(ns kithara.rabbitmq.utils
  (:require [flake.core :as flake]
            [flake.utils :refer [base62-encode]]))

(defonce __flake-init__
  (flake/init!))

(defn random-string
  []
  (base62-encode (flake/generate)))

(defn stringify-keys1
  ^java.util.Map
  [m]
  (->> (for [[k v] m]
         [(name k) v])
       (into {})))
