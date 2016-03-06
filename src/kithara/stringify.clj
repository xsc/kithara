(ns kithara.stringify)

(defn stringify-keys1
  [m]
  (->> (for [[k v] m]
         [(name k) v])
       (into {})))
