(ns kithara.rabbitmq.utils)

(defn stringify-keys1
  ^java.util.Map
  [m]
  (->> (for [[k v] m]
         [(name k) v])
       (into {})))
