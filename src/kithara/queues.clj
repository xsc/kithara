(ns kithara.queues
  (:require [kithara.rabbitmq.queue :as queue]
            [peripheral.core :refer [defcomponent]]))

;; ## Logic

(defn- declare-queue
  [{:keys [channel queue-name declare-queue?] :as opts}]
  (if declare-queue?
    (queue/declare channel queue-name opts)
    (queue/declare-passive channel queue-name)))

(defn- bind-queue
  [queue {:keys [exchange routing-keys bind-arguments declare-queue?]}]
  (when declare-queue?
    (queue/bind
      queue
      {:exchange     exchange
       :routing-keys routing-keys
       :arguments    bind-arguments})))

(defn- make-queue
  [opts]
  (doto (declare-queue opts)
    (bind-queue opts)))

(defn- make-consumers
  [{:keys [consumers queue]}]
  (map #(assoc % :queue queue) consumers))

;; ## Component

(defcomponent QueueConsumer [consumers
                             channel
                             queue-name
                             durable?
                             exclusive?
                             auto-delete?
                             arguments
                             exchange
                             routing-keys
                             bind-arguments
                             declare-queue?]
  :assert/channel?    (some? channel)
  :assert/name?       (string? queue-name)
  :this/as            *this*
  :queue              (make-queue *this*)
  :components/running (make-consumers *this*))

;; ## Wrapper

(defn with-queue
  "Bind the given consumer to the given queue."
  ([consumers queue-name]
   (with-queue consumers queue-name {:declare-queue? false}))
  ([consumers queue-name queue-options]
   (map->QueueConsumer
     (merge
       {:consumers      (if (sequential? consumers) consumers [consumers])
        :queue-name     queue-name
        :durable?       false
        :exclusive?     true
        :auto-delete?   true
        :declare-queue? true}
       queue-options))))
