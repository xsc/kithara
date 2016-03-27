(ns kithara.rabbitmq.basic-properties
  (:require [clojure.walk :as w]))

;; ## Keywordize Headers

(defprotocol ^:no-doc HeaderValue
  (to-clj [_])
  (to-rmq [_]))

(extend-protocol HeaderValue
  java.util.Map
  (to-clj [m]
    (->>  (for [[k v] m]
            [(keyword k) (to-clj v)])
         (into {})))
  (to-rmq [m]
    (->>  (for [[k v] m]
            [(name k) (to-rmq v)])
         (into {})))

  com.rabbitmq.client.LongString
  (to-clj [v]
    (str v))
  (to-rmq [v]
    v)

  java.util.List
  (to-clj [sq]
    (mapv to-clj sq))
  (to-rmq [sq]
    (mapv to-rmq sq))

  Object
  (to-clj [v] v)
  (to-rmq [v] v)

  nil
  (to-rmq [_] nil)
  (to-clj [_] nil))

;; ## Conversion

(defn from-map
  ^com.rabbitmq.client.AMQP$BasicProperties
  [{:keys [app-id
           cluster-id
           content-encoding
           content-type
           correlation-id
           delivery-mode
           expiration
           headers
           message-id
           priority
           reply-to
           timestamp
           type
           user-id]}]
  (cond-> (com.rabbitmq.client.AMQP$BasicProperties$Builder.)
    app-id           (.appId app-id)
    cluster-id       (.clusterId cluster-id)
    content-encoding (.contentEncoding content-encoding)
    correlation-id   (.correlationId correlation-id)
    delivery-mode    (.deliveryMode delivery-mode)
    expiration       (.expiration expiration)
    headers          (.headers (to-rmq headers))
    message-id       (.messageId message-id)
    priority         (.priority priority)
    reply-to         (.replyTo reply-to)
    timestamp        (.timestamp timestamp)
    type             (.type type)
    user-id          (.userId user-id)
    :finally         (.build)))

(defn to-map
  [^com.rabbitmq.client.AMQP$BasicProperties properties]
  {:app-id           (.getAppId properties)
   :cluster-id       (.getClusterId properties)
   :content-encoding (.getContentEncoding properties)
   :content-type     (.getContentType properties)
   :correlation-id   (.getCorrelationId properties)
   :delivery-mode    (.getDeliveryMode properties)
   :expiration       (.getExpiration properties)
   :headers          (to-clj (.getHeaders properties))
   :message-id       (.getMessageId properties)
   :priority         (.getPriority properties)
   :reply-to         (.getReplyTo properties)
   :timestamp        (.getTimestamp properties)
   :type             (.getType properties)
   :user-id          (.getUserId properties)})
