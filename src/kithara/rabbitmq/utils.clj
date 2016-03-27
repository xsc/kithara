(ns ^:no-doc kithara.rabbitmq.utils
  (:require [flake.core :as flake]
            [flake.utils :refer [base62-encode]]))

(defonce ^:private __flake-init__
  (delay (flake/init!)))

(defn random-string
  []
  @__flake-init__
  (base62-encode (flake/generate)))

(defn stringify-keys1
  ^java.util.Map
  [m]
  (->> (for [[k v] m]
         [(name k) v])
       (into {})))
