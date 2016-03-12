(ns kithara.rabbitmq.basic-properties
  (:require [kithara.rabbitmq.utils :as u]))

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
    headers          (.headers (u/stringify-keys1 headers))
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
   :cluser-id        (.getClusterId properties)
   :content-encoding (.getContentEncoding properties)
   :content-type     (.getContentType properties)
   :correlation-id   (.getCorrelationId properties)
   :delivery-mode    (.getDeliveryMode properties)
   :expiration       (.getExpiration properties)
   :headers          (.getHeaders properties)
   :message-id       (.getMessageId properties)
   :priority         (.getPriority properties)
   :reply-to         (.getReplyTo properties)
   :timestamp        (.getTimestamp properties)
   :type             (.getType properties)
   :user-id          (.getUserId properties)})
