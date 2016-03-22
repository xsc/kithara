(ns kithara.components.dlx-consumer
  (:require [kithara.rabbitmq
             [exchange :as exchange]
             [publish :as publisher]
             [queue :as queue]
             [utils :as u]]
            [kithara.components.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Naming

(defn- suffix-queue
  [{:keys [queue-name]} suffix]
  (str queue-name "--" (name suffix)))

(defn- make-output-exchange-name
  [{:keys [consumer-queue output-exchange]}]
  (or (:exchange-name output-exchange)
      (suffix-queue consumer-queue :retry)))

(defn- make-input-exchange-name
  [{:keys [consumer-queue input-exchange]}]
  (or (:exchange-name input-exchange)
      (suffix-queue consumer-queue :backoff)))

(defn- make-dead-letter-queue-name
  [{:keys [consumer-queue queue]}]
  (or (:queue-name queue)
      (suffix-queue consumer-queue :dead-letters)))

;; ## Declare Topology

(defn- declare-output-exchange!
  [{:keys [consumer-queue output-exchange-name output-exchange]}]
  (let [{:keys [channel]} consumer-queue]
    (exchange/declare
      channel
      output-exchange-name
      :fanout
      output-exchange)))

(defn- declare-input-exchange!
  [{:keys [consumer-queue input-exchange-name input-exchange]}]
  (let [{:keys [channel]} consumer-queue]
    (exchange/declare
      channel
      input-exchange-name
      :fanout
      input-exchange)))

(defn- declare-queue!
  [{:keys [consumer-queue
           queue-name
           output-exchange-name
           queue]}]
  (let [{:keys [channel]} consumer-queue
        opts (assoc-in queue
                       [:arguments :x-dead-letter-exchange]
                       output-exchange-name)]
    (queue/declare channel queue-name opts)))

;; ## Connect/Bind Topology

(defn- bind-dead-letter-queue!
  [{:keys [dead-letter-queue
           input-exchange-name
           output-exchange-name]}]
  (queue/bind
    dead-letter-queue
    {:exchange     input-exchange-name
     :routing-keys ["#"]}))

(defn- bind-consumer-queue!
  [{:keys [consumer-queue output-exchange-name]}]
  (queue/bind
    consumer-queue
    {:exchange     output-exchange-name
     :routing-keys ["#"]}))

(defn- bind-queues!
  [dlx-consumer]
  (doto dlx-consumer
    (bind-dead-letter-queue!)
    (bind-consumer-queue!)))

;; ## Consumer w/ Dead Letter Handling

(defn- previous-backoff
  [message]
  (some-> message
          (get-in [:properties :headers :x-death])
          first
          :original-expiration
          Long.))

(defn- calculate-backoff
  [{:keys [min max factor]
    :or {min    50
         max    60000
         factor 2}}
   message]
  (let [t  (previous-backoff message)
        r  (rand)
        t' (if t
             (if (>= t max)
               max
               (* (+ 2 r) (inc t)))
             (* (+ 1 r) (inc min)))]
    (long
      (cond (< t' min) min
            (> t' max) max
            :else t'))))

(defn- prepare-dead-message
  [{:keys [input-exchange-name backoff]} {:keys [body-raw] :as message}]
  (let [expiration (calculate-backoff backoff message)]
    (-> message
        (assoc :body body-raw, :exchange input-exchange-name)
        (assoc-in [:properties :expiration] (str expiration))
        (update-in [:properties :headers] dissoc :x-death)
        (update-in [:properties :headers :x-kithara-retries] (fnil inc 0)))))

(defn- publish-dead-message!
  [{:keys [dead-letter-queue] :as component} message]
  (->> message
       (prepare-dead-message component)
       (publisher/publish dead-letter-queue)))

(defn- handle-with-backoff
  [component handler message]
  (let [{:keys [done? nack? reject?] :as result} (handler message)
        requeue? (get result :requeue? nack?)]
    (if-not done?
      (if (and (or nack? reject?) requeue?)
        (do
          (publish-dead-message! component message)
          (assoc result :requeue? false))
        result)
      result)))

(defn- make-consumers
  [{:keys [consumers consumer-queue] :as component}]
  (-> consumers
      (p/wrap-handler
        (fn [handler]
          #(handle-with-backoff component handler %)))
      (p/set-queue consumer-queue)))

;; ## Component

(defcomponent DLXConsumer [consumers
                           consumer-queue
                           input-exchange
                           output-exchange
                           queue]
  :this/as              *this*
  :input-exchange-name  (make-input-exchange-name *this*)
  :output-exchange-name (make-output-exchange-name *this*)
  :queue-name           (make-dead-letter-queue-name *this*)
  :dead-letter-output   (declare-output-exchange! *this*)
  :dead-letter-input    (declare-input-exchange! *this*)
  :dead-letter-queue    (declare-queue! *this*)
  :components/running   (make-consumers *this*)

  :on/started (bind-queues! *this*)

  p/HasHandler
  (wrap-handler [this wrap-fn]
    (update this :consumers p/wrap-handler wrap-fn))

  p/HasQueue
  (set-queue [this queue]
    (assoc this :consumer-queue queue)))

;; ## Wrapper

(defn- as-exchange-map
  [value]
  {:pre [(or (nil? value) (map? value) (string? value))]}
  (merge
    {:durable?      false
     :auto-delete?  true}
    (if (string? value)
      {:exchange-name value}
      value)))

(defn- as-queue-map
  [value]
  {:pre [(or (nil? value) (map? value) (string? value))]}
  (merge
    {:durable?     false
     :exclusive?   true
     :auto-delete? true}
    (if (string? value)
      {:queue-name value}
      value)))

(defn with-dead-letter-backoff
  [consumers & [{:keys [input-exchange output-exchange queue]}]]
  (map->DLXConsumer
    {:consumers       (p/consumer-seq consumers)
     :input-exchange  (as-exchange-map input-exchange)
     :output-exchange (as-exchange-map output-exchange)
     :queue           (as-queue-map queue)}))
