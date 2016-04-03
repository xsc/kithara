(ns kithara.test.fixtures
  (:require [kithara.rabbitmq
             [channel :as channel]
             [connection :as connection]
             [exchange :as exchange]
             [publish :as publisher]]
            [kithara.utils :refer [random-string]]
            [clojure.test :as test]))

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

(defn track!
  [f & args]
  (swap! *tracker* #(apply f % args)))

(defn tracked
  ([]   @*tracker*)
  ([ks] (get-in @*tracker* ks)))

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
