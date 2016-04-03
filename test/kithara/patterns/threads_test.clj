(ns kithara.patterns.threads-test
  (:require [clojure.test.check
             [generators :as gen]
             [clojure-test :refer [defspec]]]
            [kithara.test :as test]
            [kithara.core-test :as core]
            [kithara.patterns.threads :refer [with-threads]]))

(test/use-rabbitmq-fixtures)

;; ## Layer

(def thread-pool-layer
  (->> gen/nat
       (gen/fmap (comp inc #(mod % 4)))
       (gen/fmap
         (fn [thread-count]
           {:build-fn (fn [consumers _] (with-threads consumers thread-count))
            :forms    [(list 'with-threads thread-count)]}))))

;; ## Tests

(defspec t-threaded-consumer 100
  (test/consumer-property
    (gen/one-of
      [;; thread-pool per consumer
       (test/stack-gen
         core/consumer-layer
         thread-pool-layer
         core/clone-layer
         core/channel-layer
         core/clone-layer
         core/queue-layer
         core/connection-layer)

       ;; thread-pool per channel
       (test/stack-gen
         core/consumer-layer
         core/clone-layer
         core/channel-layer
         thread-pool-layer
         core/clone-layer
         core/queue-layer
         core/connection-layer)

       ;; global thread-pool
       (test/stack-gen
         core/consumer-layer
         core/clone-layer
         core/channel-layer
         core/clone-layer
         core/queue-layer
         core/connection-layer
         thread-pool-layer)])))

