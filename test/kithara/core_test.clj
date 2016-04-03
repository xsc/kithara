(ns kithara.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [kithara.test :as test]
            [kithara.core :as kithara]
            [kithara.utils :refer [random-string]]))

(test/use-rabbitmq-fixtures)

;; ## Tests

(defspec t-consumer 10
  (test/basic-consumer-property
    [message-handler]
    (-> message-handler
        (kithara/consumer {:consumer-name "kithara"})
        (kithara/with-queue
          (random-string)
          {:exchange     (test/exchange-name)
           :routing-keys ["#"]})
        (kithara/with-channel {})
        (kithara/with-connection (test/connection-config)))))
