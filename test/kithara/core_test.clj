(ns kithara.core-test
  (:require [kithara.test :as test]
            [clojure.test :refer :all]
            [kithara.rabbitmq
             [connection :as c]
             [channel :as ch]
             [message :as message]
             [publish :refer [publish]]]
            [kithara.core :as kithara]
            [peripheral.core :as p]))

(test/use-rabbitmq-fixtures)

;; ## Tests

(defn make-consumer
  [handler]
  (-> handler
      (kithara/consumer {:consumer-name "kithara"})
      (kithara/with-queue
        "kithara-test-queue"
        {:exchange     (test/exchange-name)
         :routing-keys ["#"]})
      (kithara/with-channel {})
      (kithara/with-connection (test/connection-config))))

(deftest t-default-consumer
  (testing "consumer with default behaviour."
    (let [ack<?    (promise)
          nack<?   (promise)
          renack<? (promise)
          reject<? (promise)
          error<?  (promise)
          handler
          (fn [{:keys [routing-key redelivered?] :as message}]
            {:status (case [routing-key redelivered?]
                       ["reject" false]
                       (do (deliver reject<? true) :reject)
                       ["ack" false]
                       (do (deliver ack<? true) :ack)
                       ["nack" false]
                       (do (deliver nack<? true) :nack)
                       ["nack" true]
                       (do (deliver renack<? true) :ack)
                       ["error" false]
                       (throw (Exception.))
                       ["error" true]
                       (do (deliver error<? true) :ack))})]
      (p/with-start [consumer (is (make-consumer handler))]
        (testing "ack."
          (is (nil? (test/publish! "ack")))
          (is (deref ack<? 500 nil)))
        (testing "nack (w/ requeue)."
          (is (nil? (test/publish! "nack")))
          (is (deref nack<? 500 nil))
          (is (deref renack<? 500 nil)))
        (testing "reject."
          (is (nil? (test/publish! "reject")))
          (is (deref reject<? 500 nil)))
        (testing "error (w/ requeue)."
          (is (nil? (test/publish! "error")))
          (is (deref error<? 500 nil)))))))
