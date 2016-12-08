(ns kithara.rabbitmq.queue
  (:refer-clojure :exclude [declare get])
  (:require [kithara.rabbitmq
             [message :as message]
             [publish :as publisher]]
            [kithara.utils :as u]
            [potemkin :refer [defprotocol+]])
  (:import [com.rabbitmq.client
            AMQP$Queue$DeclareOk
            Channel
            GetResponse]))

;; ## Protocol

(defprotocol+ AMQPQueue
  (get [queue {:keys [auto-ack? as]
               :or {auto-ack? true
                    as :bytes}}]
    "Retrieve a single message from the queue.")
  (bind [queue {:keys [exchange routing-keys arguments]}]
    "Bind the queue to the given routing keys on the given exchange."))

;; ## Queue Logic

(defn- basic-get
  ^GetResponse
  [^Channel channel ^AMQP$Queue$DeclareOk ok auto-ack?]
  (.basicGet channel (.getQueue ok) (boolean auto-ack?)))

(defn- queue-get
  [channel ok {:keys [auto-ack? as]
               :or {auto-ack? true}
               :as opts}]
  (when-let [r (basic-get channel ok auto-ack?)]
    (message/build
      channel
      (.getEnvelope r)
      (.getProps r)
      (.getBody r)
      opts)))

(defn- queue-bind
  [^Channel channel ok {:keys [^String exchange routing-keys arguments]}]
  {:pre [exchange
         (seq routing-keys)
         (every? string? routing-keys)]}
  (let [arguments (u/stringify-keys1 arguments)
        queue-name (.getQueue ok)]
    (doseq [routing-key routing-keys]
      (.queueBind channel queue-name exchange routing-key arguments))))

;; ## Queue Type (w/ Lookup)

(deftype Queue [^Channel channel ^AMQP$Queue$DeclareOk ok]
  AMQPQueue
  (get [_ opts]
    (queue-get channel ok opts))
  (bind [_ opts]
    (queue-bind channel ok opts))

  publisher/Publisher
  (publish [_ message]
    (publisher/publish channel message))

  clojure.lang.ILookup
  (valAt [_ k]
    (case k
      :channel        channel
      :queue-name     (.getQueue ok)
      :message-count  (.getMessageCount ok)
      :consumer-count (.getConsumerCount ok)
      (throw
        (IllegalArgumentException.
          (str "cannot look up key " k " in queue map."))))))

(alter-meta! #'->Queue assoc :private true)

;; ## Declare

(defn- ->queue
  [ok channel]
  (->Queue channel ok))

(defn declare
  "Declare a new RabbitMQ queue. Note that, for named queues, options have
   to match the potentially already existing queue."
  (^Queue
    [^Channel channel]
    (->queue (.queueDeclare channel) channel))
  (^Queue
    [^Channel channel
     ^String queue-name
     & [{:keys [durable? exclusive? auto-delete? arguments]
         :or {exclusive?   true
              auto-delete? true
              durable?     false}}]]
    (let [arguments (u/stringify-keys1 arguments)]
      (-> channel
          (.queueDeclare
            queue-name
            (boolean durable?)
            (boolean exclusive?)
            (boolean auto-delete?)
            arguments)
          (->queue channel)))))

(defn declare-passive
  "Passively declare a RabbitMQ queue."
  ^Queue
  [^Channel channel ^String queue-name]
  (-> channel
      (.queueDeclarePassive queue-name)
      (->queue channel)))
