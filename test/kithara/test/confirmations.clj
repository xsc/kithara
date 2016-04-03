(ns kithara.test.confirmations
  (:require [clojure.test.check.generators :as gen]))

(def ^:private retriable-confirmation
  (gen/elements
    [{:status :nack}
     {:status :nack, :requeue? true}
     {:status :reject, :requeue? true}]))

(def ^:private final-confirmation
  (gen/elements
    [{:status :ack}
     {:status :nack, :requeue? false}
     {:status :reject}
     {:status :reject, :requeue? false}]))

(def gen
  (->> (gen/tuple
         (gen/vector retriable-confirmation 0 5)
         final-confirmation)
       (gen/fmap #(apply conj %))))
