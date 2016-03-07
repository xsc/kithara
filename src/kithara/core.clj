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
  [{:keys [consumer-tag consumer-opts queue-opts] } callback]
  (let [{:keys [auto-ack? on-error]} consumer-opts]
    (fn [message]
      (try
        (callback message)
        (catch Throwable t
          (log/errorf t "uncaught exception in consumer [%s]." consumer-tag)
          (when-not auto-ack?
            (if on-error
              (on-error message t)
              (rmq/nack message))))))))

;; ## Infrastructure

(defn- prepare-infrastructure
  [{:keys [consumer-opts] :as consumer}]
  (if-let [infrastructure (:infrastructure consumer-opts)]
    {}
    {}))

;; ## Component

(defcomponent Consumer [id
                        connection-opts
                        queue-opts
                        consumer-opts
                        callback]
  :this/as         *this*
  :consumer-tag    (prepare-consumer-tag id)

  :connection-opts (merge default-connection-opts connection-opts)
  :connection      (rmq/connect connection-opts) #(rmq/disconnect %)

  :channel-opts    (merge default-channel-opts consumer-opts)
  :channel         (rmq/open connection channel-opts) #(rmq/close %)

  :component/infra (prepare-infrastructure *this*)

  :queue-opts      (merge default-queue-opts queue-opts)
  :queue           (prepare-queue channel queue-opts)

  :callback'
  (prepare-callback *this* callback)
  :consumer
  (->> {:consumer-tag consumer-tag}
       (merge consumer-opts)
       (rmq/consume queue callback'))
  #(rmq/cancel %))

;; ## Constructor

(defn consumer
  [{:keys [consumer-id
           connection
           channel
           queue
           consumer]}]
  {:pre [(map? connection)
         (or (not (:declare? queue)) (string? (:exchange queue)))
         (string? consumer-id)
         (or (seq (:routing-keys queue)) (:routing-key queue))
         (:callback consumer)]}
  (map->Consumer
    {:connection-opts connection
     :queue-opts    queue
     :consumer-opts consumer
     :id            consumer-id
     :callback      (:callback consumer)}))
