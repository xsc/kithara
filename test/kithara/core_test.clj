(ns kithara.core-test
  (:require [clojure.test :refer :all]
            [kithara.core :as kithara]
            [kithara.rabbitmq :as rmq]
            [peripheral.core :as p]))

;; ## Connection Data

(def ^:private rabbitmq-host
  (or (System/getenv "RABBITMQ_HOST") "localhost"))

(def ^:private rabbitmq-port
  (Long. (or (System/getenv "RABBITMQ_PORT") "5672")))

(def ^:private connection
  {:host rabbitmq-host
   :port rabbitmq-port})

(def ^:private queue-name
  "kithara-test")

(def ^:private exchange-name
  "kithara-test-exchange")

;; ## Fixtures

(defmacro ^:private with-close
  [[sym open close & more] & body]
  `(let [c# ~open
         ~sym c#]
     (try
       ~(if (seq more)
          `(with-close [~@more] ~@body)
          `(do ~@body))
       (finally
         (~close c#)))))

(defn- declare-exchange
  [channel]
  (.exchangeDeclare channel exchange-name "topic")
  channel)

(defn- delete-exchange
  [channel]
  (.exchangeDelete channel exchange-name))

(use-fixtures
  :once
  (fn [f]
    (with-close
      [conn     (rmq/connect connection)   rmq/disconnect
       channel  (rmq/open conn)            rmq/close
       exchange (declare-exchange channel) delete-exchange]
      (f))))

;; ## Tests

(defn make-consumer
  [consumer]
  (kithara/consumer
    {:consumer-id "kithara"
     :connection  connection
     :queue       {:queue-name  queue-name
                   :exchange    exchange-name
                   :routing-key "#"}
     :consumer    consumer}))

(defn- publish!
  [consumer routing-key & [properties]]
  (rmq/publish
    (:channel consumer)
    {:exchange exchange-name
     :routing-key routing-key
     :properties properties
     :body (.getBytes routing-key "UTF-8")}))

(deftest t-default-consumer
  (testing "consumer with default behaviour."
    (let [ack<?    (promise)
          nack<?   (promise)
          renack<? (promise)
          reject<? (promise)
          error<?  (promise)
          callback
          (fn [{:keys [routing-key redelivered?] :as message}]
            (case [routing-key redelivered?]
              ["reject" false]
              (do (deliver reject<? true) (rmq/reject message))
              ["ack" false]
              (do (deliver ack<? true) (rmq/ack message))
              ["nack" false]
              (do (deliver nack<? true) (rmq/nack message))
              ["nack" true]
              (do (deliver renack<? true) (rmq/ack message))
              ["error" false]
              (throw (Exception.))
              ["error" true]
              (do (deliver error<? true) (rmq/ack message))))]
      (p/with-start [consumer (is (make-consumer {:callback callback}))]
        (testing "ack."
          (is (nil? (publish! consumer "ack")))
          (is (deref ack<? 500 nil)))
        (testing "nack (w/ requeue)."
          (is (nil? (publish! consumer "nack")))
          (is (deref nack<? 500 nil))
          (is (deref renack<? 500 nil)))
        (testing "reject."
          (is (nil? (publish! consumer "reject")))
          (is (deref reject<? 500 nil)))
        (testing "error (w/ requeue)."
          (is (nil? (publish! consumer "error")))
          (is (deref error<? 500 nil)))))))
