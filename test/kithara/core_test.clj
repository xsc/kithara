(ns kithara.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check
             [generators :as gen]
             [clojure-test :refer [defspec]]]
            [kithara.test :as test]
            [kithara.core :as kithara]
            [kithara.utils :refer [random-string]]))

(test/use-rabbitmq-fixtures)

;; ## Tests

(defspec t-consumer 10
  (test/consumer-property
    (fn [message-handler connection exchange]
      (-> message-handler
          (kithara/consumer {:consumer-name "kithara"})
          (kithara/with-queue
            (random-string)
            {:exchange     exchange
             :routing-keys ["#"]})
          (kithara/with-channel {})
          (kithara/with-connection connection)))))
