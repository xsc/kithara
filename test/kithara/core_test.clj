(ns kithara.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check
             [generators :as gen]
             [clojure-test :refer [defspec]]]
            [kithara.test :as test]
            [kithara.core :as kithara]
            [kithara.utils :refer [random-string]]))

(test/use-rabbitmq-fixtures)

;; ## Stack Layers

;; ### Clone Layer

(def clone-layer
  (->> gen/nat
       (gen/fmap #(mod % 3))
       (gen/fmap inc)
       (gen/fmap
         (fn [multiplier]
           {:build-fn (fn [consumer _]
                        (if (= multiplier 1)
                          consumer
                          (repeat multiplier consumer)))
            :forms (if (> multiplier 1)
                     [(list 'clone multiplier)])}))))

;; ### Consumer Layer

(def consumer-layer
  (test/stack-elements
    [message-handler _]
    (kithara/consumer message-handler)
    (kithara/consumer message-handler {:consumer-name (random-string)})))

;; ### Channel Layer

(def channel-layer
  (gen/let [prefetch-count (gen/fmap #(+ (mod % 1024) 128) gen/nat)]
    (test/stack-elements
      [consumers _]
      (kithara/with-channel consumers)
      (kithara/with-prefetch-channel consumers prefetch-count))))

;; ### Queue Layer

(defn- ->bindings
  [{:keys [exchange]}]
  {:exchange     exchange
   :routing-keys ["#"]})

(def queue-layer
  (test/stack-elements
    [consumers {:keys [queue] :as options}]
    (kithara/with-server-named-queue consumers (->bindings options))
    (kithara/with-queue consumers queue (->bindings options))))

;; ### Connection Layer

(def connection-layer
  (test/stack-elements
    [consumers {:keys [connection]}]
    (kithara/with-connection consumers connection)))

;; ## Tests

(defspec t-basic-consumers 100
  (test/consumer-property
    (test/stack-gen
      consumer-layer
      clone-layer
      channel-layer
      clone-layer
      queue-layer
      connection-layer)))
