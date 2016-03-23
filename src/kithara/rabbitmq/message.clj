(ns kithara.rabbitmq.message
  (:require [kithara.rabbitmq.basic-properties :as basic-properties]
            [kithara.protocols :as p]
            [potemkin :refer [defprotocol+]])
  (:import [com.rabbitmq.client Channel Envelope]))

;; ## Message Operations

(defn ack
  "ACK the given message."
  [{:keys [^Channel channel ^long delivery-tag]}]
  (.basicAck channel delivery-tag false))

(defn ack-multiple
  "ACK the given message (or multiple ones)."
  [{:keys [^Channel channel ^long delivery-tag]}]
  (.basicAck channel delivery-tag true))

(defn nack
  "NACK the given message, requeueing if desired."
  [{:keys [^Channel channel ^long delivery-tag requeue?]
    :or {requeue? true}}]
  (.basicNack channel delivery-tag false (boolean requeue?)))

(defn nack-multiple
  "NACK the given message (or multiple ones), requeueing if desired."
  [{:keys [^Channel channel ^long delivery-tag requeue?]
    :or {requeue? true}}]
  (.basicNack channel delivery-tag true (boolean requeue?)))

(defn reject
  "REJECT the given message, requeing if desired."
  [{:keys [^Channel channel ^long delivery-tag requeue?]
    :or {requeue? false}}]
  (.basicReject channel delivery-tag (boolean requeue?)))

;; ## Envelope

(defn- envelope-as-map
  [^Envelope envelope]
  {:routing-key  (.getRoutingKey envelope)
   :exchange     (.getExchange envelope)
   :redelivered? (.isRedeliver envelope)
   :delivery-tag (.getDeliveryTag envelope)})

;; ## Build

(defn build
  "Generate the message map based on the given data/options. The map
   will contain the following keys:

   - `:channel`: the channel the message was received on,
   - `:exchange`: the exchange the message was published to,
   - `:routing-key`: the message's routing key,
   - `:body`: the coerced body,
   - `:body-raw`: the body as a byte array,
   - `:properties`: a map of message properties,
   - `:redelivered?`: whether the message was redelivered,
   - `:delivery-tag`: the message's delivery tag.

   The resulting map can be passed as-is to ACK/NACK/REJECT functions."
  [^Channel channel envelope properties body
   & [{:keys [as] :or {as :bytes}}]]
  (merge
    {:channel       channel
     :body          (p/coerce as body)
     :body-raw      body
     :properties    (basic-properties/to-map properties)}
    (envelope-as-map envelope)))
