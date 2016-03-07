(ns kithara.rabbitmq
  (:refer-clojure :exclude [declare get])
  (:require [kithara.basic-properties :as basic-properties]
            [kithara.config :as config]
            [kithara.stringify :refer [stringify-keys1]])
  (:import [net.jodah.lyra Connections ConnectionOptions]
           [net.jodah.lyra.config Config]
           [com.rabbitmq.client Connection Channel]))

;; ## Connection

(defn connect
  (^com.rabbitmq.client.Connection
    [options]
    (connect
      (config/connection options)
      (config/build options)))
  (^com.rabbitmq.client.Connection
    [^ConnectionOptions connection-options ^Config config]
    (Connections/create connection-options config)))

(defn disconnect
  [^Connection c]
  (.close c))

;; ## Channel

;; ### Open/Close

(defn open
  ^com.rabbitmq.client.Channel
  [^Connection c &
   [{:keys [channel-number
            prefetch-count
            prefetch-size
            prefetch-global?]}]]
  (when-let [ch (if channel-number
                  (.createChannel c (int channel-number))
                  (.createChannel c))]
    (when prefetch-count
      (cond (and prefetch-size prefetch-global?)
            (.basicQos
              ch
              (int prefetch-count)
              (int prefetch-size)
              (boolean prefetch-global?))
            prefetch-size
            (.basicQos ch (int prefetch-count) (int prefetch-size))
            :else (.basicQos ch (int prefetch-count))))
    ch))

(defn close
  ([^Channel channel]
   (.close channel))
  ([^Channel channel close-code close-message]
   (.close channel (int close-code) (str close-message))))

;; ### Publish

(defn publish
  [^Channel channel
   {:keys [exchange
           routing-key
           mandatory?
           immediate?
           properties
           ^bytes body]}]
  {:pre [(string? exchange)
         (string? routing-key)
         body]}
  (let [properties (basic-properties/from-map properties)]
    (cond (not-any? nil? [mandatory? immediate?])
          (.basicPublish
            channel exchange routing-key mandatory? immediate? properties body)

          (some? mandatory?)
          (.basicPublish
            channel exchange routing-key mandatory? properties body)

          (some? immediate?)
          (.basicPublish
            channel exchange routing-key false immediate? properties body)

          :else
          (.basicPublish
            channel exchange routing-key properties body))))

;; ### Get

(defn- read-envelope
  [^com.rabbitmq.client.Envelope envelope]
  {:routing-key  (.getRoutingKey envelope)
   :exchange     (.getExchange envelope)
   :redelivered? (.isRedeliver envelope)
   :delivery-tag (.getDeliveryTag envelope)})

(defn- read-body
  [^bytes body as]
  (cond-> body
    (= as :string) (String. "UTF-8")
    (fn? as)       (as)))

(defn get
  [{:keys [^Channel channel queue-name] :as queue}
   & [{:keys [auto-ack? as]
       :or {auto-ack? true
            as        :bytes}}]]
  (when-let [^com.rabbitmq.client.GetResponse response
             (.basicGet channel queue-name (boolean auto-ack?))]
    (let [envelope   (.getEnvelope response)
          body       (.getBody response)
          properties (.getProps response)]
      (merge
        {:channel       channel
         :body          (read-body body)
         :message-count (.getMessageCount response)
         :properties    (basic-properties/to-map properties)}
        (read-envelope)))))

;; ### Ack/Nack/Reject

(defn ack
  [{:keys [^Channel channel ^long delivery-tag]}]
  (.basicAck channel delivery-tag false))

(defn ack-multiple
  [{:keys [^Channel channel ^long delivery-tag]}]
  (.basicAck channel delivery-tag true))

(defn nack
  [{:keys [^Channel channel ^long delivery-tag requeue?]
    :or {requeue? true}}]
  (.basicNack channel delivery-tag false (boolean requeue?)))

(defn nack-multiple
  [{:keys [^Channel channel ^long delivery-tag requeue?]
    :or {requeue? true}}]
  (.basicNack channel delivery-tag true (boolean requeue?)))

(defn reject
  [{:keys [^Channel channel ^long delivery-tag requeue?]
    :or {requeue? false}}]
  (.basicReject channel delivery-tag (boolean requeue?)))

;; ## Queue

(deftype Queue [^Channel channel
                ^com.rabbitmq.client.AMQP$Queue$DeclareOk ok]
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

(defn- make-queue
  [^com.rabbitmq.client.AMQP$Queue$DeclareOk ok ^Channel channel]
  (->Queue channel ok))

(defn declare
  ([^Channel channel]
   (-> channel
       (.queueDeclare)
       (make-queue channel)))
  ([^Channel channel queue-name
    & [{:keys [durable? exclusive? auto-delete? arguments]
        :or {exclusive?   true
             auto-delete? true
             durable?     true}
        :as opts}]]
   (let [arguments (stringify-keys1 arguments)]
     (-> channel
         (.queueDeclare
           (str queue-name)
           (boolean durable?)
           (boolean exclusive?)
           (boolean auto-delete?)
           arguments)
         (make-queue channel)))))

(defn declare-passive
  [^Channel channel queue-name]
  (-> channel
      (.queueDeclarePassive (str queue-name))
      (make-queue channel)))

(defn delete
  [{:keys [^Channel channel queue-name]}]
  (.queueDelete channel (str queue-name)))

(defn bind
  [{:keys [^Channel channel queue-name] :as queue}
   {:keys [exchange routing-key routing-keys arguments] :as opts}]
  {:pre [(or (seq routing-keys) routing-key)]}
  (let [arguments (stringify-keys1 arguments)
        routing-keys (or (seq routing-keys) [routing-key])]
    (doseq [routing-key routing-keys]
      (.queueBind channel queue-name exchange routing-key arguments))))

;; ## Consumer

;; ### Delivery Handler

(defn- handle-delivery
  [{:keys [channel as]} f consumer-tag envelope properties body]
  (let [message (merge
                  {:channel      channel
                   :consumer-tag consumer-tag
                   :properties   (basic-properties/to-map properties)
                   :body         (read-body body as)}
                  (read-envelope envelope))]
    (f message)))

;; ### Consumer Callback

(defn- build-consumer
  ^com.rabbitmq.client.Consumer
  [opts f]
  (if-not (instance? com.rabbitmq.client.Consumer f)
    (reify com.rabbitmq.client.Consumer
      (handleCancel [_ _])
      (handleCancelOk [_ _])
      (handleConsumeOk [_ _])
      (handleRecoverOk [_ _])
      (handleShutdownSignal [_ _ _])
      (handleDelivery [_ consumer-tag envelope properties body]
        (handle-delivery opts f consumer-tag envelope properties body)))
    f))

;; ### Options

(defn- read-consumer-opts
  [{:keys [channel]} opts]
  (-> (merge
        {:auto-ack?  false
         :as         :bytes
         :locale?    false
         :exclusive? false}
        opts
        {:channel channel})
      (update :arguments stringify-keys1)))

;; ### Basic Consume

(defn- basic-consume
  [{:keys [^Channel channel queue-name]}
   {:keys [auto-ack? consumer-tag local? exclusive? arguments]}
   ^com.rabbitmq.client.Consumer consumer]
  (if consumer-tag
    (.basicConsume
      channel
      queue-name
      (boolean auto-ack?)
      consumer-tag
      (not local?)
      (boolean exclusive?)
      arguments
      consumer)
    (.basicConsume
      channel
      queue-name
      (boolean auto-ack?)
      arguments
      consumer)))

;; ### Start Consumer

(defn consume
  ([{:keys [^Channel channel queue-name] :as queue} callback]
   (let [opts (read-consumer-opts queue {})]
     (.basicConsume channel queue-name (build-consumer opts callback))))
  ([queue callback
    & [{:keys [as auto-ack? consumer-tag local? exclusive? arguments]
        :or {auto-ack?  false
             as         :bytes
             local?     false
             exclusive? false}
        :as opts}]]
   (let [opts      (read-consumer-opts queue opts)
         consumer  (build-consumer opts callback)]
     (basic-consume queue opts consumer))))
