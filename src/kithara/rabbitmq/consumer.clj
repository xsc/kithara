(ns kithara.rabbitmq.consumer
  (:require [kithara.rabbitmq
             [message :as message]
             [queue :as queue]
             [utils :as u]]
            [potemkin :refer [defprotocol+]])
  (:import [com.rabbitmq.client Channel Consumer]))

;; ## Protocol

(defprotocol+ Consumable
  (channel [consumable]
    "Return the underlying channel for this consumable.")
  (queue-name [consumable]
    "Return the queue name (if bound) for this consumable."))

;; ## Implementation

(defn- basic-consume
  [^Channel channel
   {:keys [^String queue-name
           ^String consumer-tag
           auto-ack?
           local?
           exclusive?
           arguments]
    :or {auto-ack?  true
         local?     false
         exclusive? false}}
   ^Consumer consumer]
  {:pre [(string? queue-name) consumer]}
  (let [arguments (u/stringify-keys1 arguments)]
    (if consumer-tag
      (.basicConsume channel
                     queue-name
                     (boolean auto-ack?)
                     consumer-tag
                     (not local?)
                     (boolean exclusive?)
                     arguments
                     consumer)
      (.basicConsume channel
                     queue-name
                     (boolean auto-ack?)
                     arguments
                     consumer))))

(defn- attach-queue-name
  [opts consumable]
  (update opts :queue-name #(or % (queue-name consumable))))

(defn consume
  "Consume from the given consumable. Options include:

   - `:auto-ack?` (defaults to true),
   - `:local?` (defaults to false),
   - `:exclusive?` (defaults to false),
   - `:consumer-tag`,
   - `:arguments`,
   - `:queue-name` (if the consumable is not bound to one).

   Returns a map of `:channel`, `:queue-name` and `:consumer-tag`."
  ([consumable ^Consumer consumer]
   (let [^Channel channel   (channel consumable)
         ^String queue-name (queue-name consumable)]
     {:consumer-tag (.basicConsume channel queue-name consumer)
      :queue-name   queue-name
      :channel      channel}))
  ([consumable opts ^Consumer consumer]
   (let [channel (channel consumable)
         opts (attach-queue-name opts consumable)]
     {:consumer-tag (basic-consume channel opts consumer)
      :queue-name   (:queue-name opts)
      :channel      channel})))

(defn cancel
  "Using the return value of `consume`, cancel the respective consumer."
  [{:keys [^Channel channel ^String consumer-tag]}]
  (.basicCancel channel consumer-tag))

;; ## Channel/Queue

(extend-protocol Consumable
  Channel
  (channel [this] this)
  (queue-name [_])

  kithara.rabbitmq.queue.Queue
  (channel [{:keys [channel]}] channel)
  (queue-name [{:keys [queue-name]}] queue-name))

;; ## Function as Consumer

(defn from-fn
  "Generate a consumer for the given consumable based on the given function.
   `f` will be called on a message map compatible with kithara's ACK/NACK/REJECT
   functions and coercer options."
  ^Consumer
  [f consumable & [opts]]
  (let [channel (channel consumable)]
    (reify Consumer
      (handleCancel [_ _])
      (handleCancelOk [_ _])
      (handleConsumeOk [_ _])
      (handleRecoverOk [_ _])
      (handleShutdownSignal [_ _ _])
      (handleDelivery [_ consumer-tag envelope properties body]
        (let [message (-> (message/build channel envelope properties body opts)
                          (assoc :consumer-tag consumer-tag))]
          (f message))))))
