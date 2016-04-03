(ns kithara.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check
             [generators :as gen]
             [clojure-test :refer [defspec]]]
            [kithara.test :as test]
            [kithara.core :as kithara]
            [kithara.utils :refer [random-string]]))

(test/use-rabbitmq-fixtures)

;; ## Generate a Stack

;; ### Helper

(defn maybe-seq
  ([element-gen]
   (maybe-seq element-gen 2))
  ([element-gen max-count]
   (gen/let [c (gen/fmap (comp inc #(mod % max-count)) gen/nat)]
     (if (= c 1)
       element-gen
       (->> (gen/vector element-gen c)
            (gen/fmap
              (fn [fs]
                (fn [& args]
                  (mapv #(apply % args) fs)))))))))

(defmacro gen-fn-elements
  [bindings & options]
  `(gen/elements
     [~@(for [form options]
          `(fn [~@bindings] ~form))]))

;; ### Consumer(s)

(def consumer-gen
  (maybe-seq
    (gen-fn-elements
      [message-handler & _]
      (kithara/consumer message-handler)
      (kithara/consumer message-handler {:consumer-name (random-string)}))))

;; ### Queue

(defn- ->bindings
  [exchange]
  {:exchange     exchange
   :routing-keys ["#"]})

(def queue-gen
  (gen-fn-elements
    [consumers _ exchange]
    (kithara/with-server-named-queue consumers (->bindings exchange))
    (kithara/with-queue consumers (random-string) (->bindings exchange))))

;; ### Channel(s)

(def channel-gen
  (maybe-seq
    (gen/let [prefetch-count (gen/fmap #(+ (mod % 1024) 128) gen/nat)]
      (gen-fn-elements
        [consumers & _]
        (kithara/with-channel consumers)
        (kithara/with-channel consumers {:channel-index 1})
        (kithara/with-prefetch-channel consumers prefetch-count)))))

;; ### Connection

(def connection-gen
  (gen-fn-elements
    [consumers connection & _]
    (kithara/with-connection consumers connection)))

;; ### Bring it together!

(def consumer-stack-gen
  "Generate a consumer stack based on a single queue."
  (gen/let [make-consumer   consumer-gen
            make-queue      queue-gen
            make-channel    channel-gen
            make-connection connection-gen]
    (gen/return
      (fn [message-handler connection exchange]
        (reduce
          #(%2 %1 connection exchange)
          message-handler
          [make-consumer
           make-channel
           make-queue
           make-connection])))))

;; ## Tests

(defspec t-consumer 100
  (gen/bind
    consumer-stack-gen
    test/consumer-property))
