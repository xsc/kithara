(ns kithara.core
  (:require [kithara.rabbitmq :as rmq]
            [peripheral.core :refer [defcomponent]]
            [clojure.tools.logging :as log])
  (:import [java.util UUID]))

;; ## Default Policies/Options

(def ^:private default-connection-opts
  {:recovery-policy
   {:backoff {:min 100, :max 180000}
    :max-attempts :always}
   :retry-policy
   {:backoff {:min 100, :max 300000}
    :max-attempts :always}
   :queue-recovery? true
   :consumer-recovery? true})

(def ^:private default-channel-opts
  {:prefetch-count   256
   :prefetch-global? false})

(def ^:private default-queue-opts
  {:declare? true})

;; ## Queue + Bindings

(defn- prepare-queue
  [channel {:keys [declare? queue-name] :as opts}]
  (if-let [q (if declare?
               (rmq/declare channel queue-name opts)
               (rmq/declare-passive channel queue-name))]
    (doto q (rmq/bind opts))
    (throw
      (IllegalStateException.
        (if declare?
          (format "could not declare queue '%s'." queue-name)
          (format "queue '%s' does not exist." queue-name))))))

;; ## Consumer Tag

(defn- prepare-consumer-tag
  [consumer-id]
  (str (or consumer-id "kithara") "-" (UUID/randomUUID)))

;; ## Consumer Callback

(defn- prepare-callback
  [consumer-tag {:keys [auto-ack?]} callback]
  (fn [message]
    (try
      (callback message)
      (catch Throwable t
        (log/errorf t "uncaught exception in consumer [%s]." consumer-tag)
        (when-not auto-ack?
          (rmq/nack message))))))

;; ## Component

(defcomponent ConnectedConsumer [connection
                                 id
                                 queue-opts
                                 consumer-opts
                                 callback]
  :channel-opts (merge default-channel-opts consumer-opts)
  :channel      (rmq/open connection channel-opts) #(rmq/close %)
  :queue-opts   (merge default-queue-opts queue-opts)
  :queue        (prepare-queue channel queue-opts)
  :consumer-tag (prepare-consumer-tag id)
  :callback'    (prepare-callback consumer-tag consumer-opts callback)
  :consumer     (log/debugf "starting consumer [%s] ..." consumer-tag)
  :consumer
  (->> {:consumer-tag consumer-tag}
       (merge consumer-opts)
       (rmq/consume queue callback')))

(defcomponent Consumer [id
                        consumer-opts
                        connection-opts
                        queue-opts
                        callback]
  :connection-opts (merge default-connection-opts connection-opts)
  :connection      (rmq/connect connection-opts) #(rmq/disconnect %)
  :component/consumer
  (map->ConnectedConsumer
    {:connection    connection
     :consumer-opts consumer-opts
     :id            id
     :queue-opts    queue-opts
     :callback      callback})
  :channel (:channel consumer))

;; ## Constructor

(defn- connection-instance?
  [connection]
  (or (nil? connection)
      (instance? com.rabbitmq.client.Connection connection)))

(defn create
  [{:keys [consumer-id
           connection
           channel
           queue
           consumer]}]
  {:pre [(or (map? connection) (connection-instance? connection))
         (or (not (:declare? queue)) (string? (:exchange queue)))
         (string? consumer-id)
         (or (seq (:routing-keys queue)) (:routing-key queue))
         (:callback consumer)]}
  (let [base {:queue-opts    queue
              :consumer-opts consumer
              :id            consumer-id
              :callback      (:callback consumer)}]
    (if (connection-instance? connection)
      (map->ConnectedConsumer
        (merge
          {:connection connection}
          base))
      (map->Consumer
        (merge
          {:connection-opts connection}
          base)))))

;; ## Specialized Constructors

(defn create-persistent
  [{:keys [queue] :as opts}]
  {:pre [(string? (:queue-name queue))]}
  (-> opts
      (update :queue merge {:exclusive?   false
                            :auto-delete? false
                            :durable?     true})
      (create)))
