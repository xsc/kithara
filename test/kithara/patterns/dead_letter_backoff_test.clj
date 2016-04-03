(ns kithara.patterns.dead-letter-backoff-test
  (:require [clojure.test.check
             [generators :as gen]
             [clojure-test :refer [defspec]]]
            [kithara.test :as test]
            [kithara.core-test :as core]
            [kithara.patterns.dead-letter-backoff
             :refer [with-dead-letter-backoff]]))

(test/use-rabbitmq-fixtures)

;; ## Layer

(def backoff-layer
  (test/stack-elements
    [consumers {:keys [queue]}]
    (with-dead-letter-backoff consumers {:backoff {:max 200}})))

;; ## Tests

(defspec t-dead-letter-backoff-consumers 30
  (test/consumer-property
    {:wait-ms 10000}
    (test/stack-gen
      core/consumer-layer
      core/clone-layer
      core/channel-layer
      core/clone-layer
      backoff-layer
      core/named-queue-layer
      core/connection-layer)))
