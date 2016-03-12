(ns kithara.rabbitmq.publish
  (:require [kithara.rabbitmq.basic-properties :as basic-properties]
            [potemkin :refer [defprotocol+]])
  (:import [com.rabbitmq.client Connection Channel]))

;; ## Protocol

(defprotocol+ Publisher
  (publish [publisher {:keys [exchange
                              routing-key
                              mandatory?
                              immediate?
                              properties
                              body]}]
    "Publish the given message."))

;; ## Channel Publishing

(defn- publish-to-channel
  [^Channel channel
   {:keys [^String exchange
           ^String routing-key
           ^bytes body
           mandatory?
           immediate?
           properties]
    :or {mandatory? false
         immediate? false}}]
  {:pre [(string? exchange)
         (string? routing-key)
         body]}
  (.basicPublish channel
                 exchange
                 routing-key
                 (boolean mandatory?)
                 (boolean immediate?)
                 (basic-properties/from-map properties)
                 body))

(extend-protocol Publisher
  Channel
  (publish [channel message]
    (publish-to-channel channel message)))
