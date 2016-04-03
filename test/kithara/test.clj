(ns kithara.test
  (:require [kithara.rabbitmq
             [channel :as channel]
             [connection :as connection]
             [exchange :as exchange]
             [publish :as publisher]]
            [kithara.utils :refer [random-string]]
            [peripheral.core :as p]
            [clojure.test.check
             [generators :as gen]
             [properties :as prop]]
            [clojure.test :as test])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

;; ## Dynamic Variables

(def ^:dynamic *connection* nil)
(def ^:dynamic *channel*    nil)
(def ^:dynamic *exchange*   nil)
(def ^:dynamic *tracker*    nil)

;; ## Data

(def ^:private rabbitmq-host
  (or (System/getenv "RABBITMQ_HOST") "docker"))

(def ^:private rabbitmq-port
  (Long. (or (System/getenv "RABBITMQ_PORT") "5672")))

(defn connection-config
  []
  {:host rabbitmq-host
   :port rabbitmq-port})

(defn exchange-name
  []
  (:exchange-name *exchange*))

;; ## Setup/Teardown

;; ### Helper

(defmacro ^:private with-setup-teardown
  [[sym setup teardown] & body]
  `(let [v# ~setup]
     (try
       (binding [~sym v#]
         ~@body)
       (finally
         (~teardown v#)))))

;; ### Connection

(defn connection-fixture
  [f]
  (with-setup-teardown [*connection*
                        (connection/open (connection-config))
                        connection/close]
    (f)))

;; ### Channel

(defn channel-fixture
  [f]
  (with-setup-teardown [*channel*
                        (channel/open *connection*)
                        channel/close]
    (f)))

;; ### Exchange

(defn exchange-fixture
  [f]
  (with-setup-teardown [*exchange*
                        (exchange/declare *channel* (random-string) :fanout)
                        exchange/delete]
    (f)))

;; ### Tracker

(defn tracker-fixture
  [f]
  (binding [*tracker* (atom {})]
    (f)))

;; ### Fixtures

(defmacro use-rabbitmq-fixtures
  []
  `(do
     (test/use-fixtures
       :once
       (reduce test/compose-fixtures
               [connection-fixture
                channel-fixture
                exchange-fixture]))
     (test/use-fixtures :each tracker-fixture)))

;; ## Publish

(defn publish!
  ([routing-key]
   (publish! routing-key (.getBytes routing-key "UTF-8")))
  ([routing-key data & [properties]]
   (publisher/publish
     *channel*
     {:exchange    (exchange-name)
      :routing-key routing-key
      :properties  properties
      :body        data})))

;; ## Generic Testcase
;;
;; Structure:
;;
;; - each message gets a description of how to handle it as body,
;; - each published message gets inserted into *tracker* with a CountDownLatch
;;   describing how often the consumer is supposed to see it,
;; - the consumer decrements the `CountDownLatch`,
;; - the consumer writes the confirmation value into `*tracker*`.

(defn make-message-handler
  [handler]
  (bound-fn [{:keys [routing-key body-raw redelivered?] :as message}]
    (let [data   (-> body-raw (String. "UTF-8") read-string)
          result (handler (assoc message :data data))]
      (swap! *tracker* update-in [routing-key :results] conj result)
      (.countDown ^CountDownLatch (get-in @*tracker* [routing-key :latch]))
      result)))

(defn publish-test-message!
  [data expected-results]
  (let [routing-key (random-string)]
    (swap! *tracker* assoc routing-key
           {:latch    (CountDownLatch. (count expected-results))
            :results  []
            :original data
            :expected expected-results})
    (publish! routing-key (-> data pr-str (.getBytes "UTF-8")))))

(defmacro wait-for-test-messages!
  [wait-ms]
  `(let [ms# ~wait-ms
         fut# (future
                (doseq [~'[routing-key {:keys [original latch]}] @*tracker*]
                  (assert
                    (.await ~'latch ms# TimeUnit/MILLISECONDS)
                    (format "message '%s' was not consumed within %dms: %s"
                            ~'routing-key
                            ms#
                            (pr-str ~'original)))))]
     (Thread/sleep (quot ms# 10))
     (assert (not= ::timeout (deref fut# ms# ::timeout))
             (format "test messages failed to be consumed within %dms."
                     ms#))
     true))

(defmacro verify-results!
  []
  `(->> (for [[~'routing-key {:keys ~'[original expected results]}] @*tracker*]
          (= ~'expected ~'results))
        (every? true?)))

(defmacro consumer-property
  [{:keys [handler builder message-gen]}]
  `(prop/for-all
     [~'messages (gen/vector ~message-gen 1 5)]
     (let [h# (make-message-handler ~handler)]
       (p/with-start [~'consumer (~builder h#)]
         (doseq [{:keys ~'[data expected]} ~'messages]
           (publish-test-message! ~'data ~'expected))
         (and (wait-for-test-messages! 2000)
              (verify-results!))))))

;; ## Default Testcase

(defn default-test-handler
  [{:keys [routing-key data]} state]
  (let [seen (-> state
                 (swap! update routing-key (fnil inc -1))
                 (get routing-key))]
    (get data seen {:status :ack})))

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

(def default-message-gen
  (->> (gen/tuple
         (gen/vector retriable-confirmation 0 5)
         final-confirmation)
       (gen/fmap #(apply conj %))
       (gen/fmap
         (fn [confirmations]
           {:expected confirmations
            :data     (zipmap (range) confirmations)}))))

(defmacro basic-consumer-property
  [[handler-symbol] & builder]
  `(let [state# (atom {})]
     (consumer-property
       {:message-gen default-message-gen
        :handler     #(default-test-handler % state#)
        :builder     (fn [~handler-symbol] ~@builder)})))
