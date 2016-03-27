(ns kithara.patterns.dead-letter-backoff
  (:require [kithara.rabbitmq
             [exchange :as exchange]
             [publish :as publisher]
             [queue :as queue]
             [utils :as u]]
            [kithara.protocols :as p]
            [clojure.tools.logging :as log]
            [peripheral.core :refer [defcomponent]]))

;; ## Naming

(defn- suffix-queue
  [{:keys [queue-name]} suffix]
  (str queue-name "--" (name suffix)))

(defn- make-retry-exchange-name
  [{:keys [consumer-queue retry-exchange]}]
  (or (:exchange-name retry-exchange)
      (suffix-queue consumer-queue :retry)))

(defn- make-backoff-exchange-name
  [{:keys [consumer-queue backoff-exchange]}]
  (or (:exchange-name backoff-exchange)
      (suffix-queue consumer-queue :backoff)))

(defn- make-dead-letter-queue-name
  [{:keys [consumer-queue queue]}]
  (or (:queue-name queue)
      (suffix-queue consumer-queue :dead-letters)))

;; ## Declare Topology

(defn- declare-retry-exchange!
  [{:keys [consumer-queue retry-exchange-name retry-exchange]}]
  (let [{:keys [channel]} consumer-queue]
    (exchange/declare
      channel
      retry-exchange-name
      :fanout
      retry-exchange)))

(defn- declare-backoff-exchange!
  [{:keys [consumer-queue backoff-exchange-name backoff-exchange]}]
  (let [{:keys [channel]} consumer-queue]
    (exchange/declare
      channel
      backoff-exchange-name
      :fanout
      backoff-exchange)))

(defn- declare-queue!
  [{:keys [consumer-queue
           queue-name
           retry-exchange-name
           queue]}]
  (let [{:keys [channel]} consumer-queue
        opts (assoc-in queue
                       [:arguments :x-dead-letter-exchange]
                       retry-exchange-name)]
    (queue/declare channel queue-name opts)))

;; ## Connect/Bind Topology

(defn- bind-dead-letter-queue!
  [{:keys [dead-letter-queue
           backoff-exchange-name
           retry-exchange-name]}]
  (queue/bind
    dead-letter-queue
    {:exchange     backoff-exchange-name
     :routing-keys ["#"]}))

(defn- bind-consumer-queue!
  [{:keys [consumer-queue retry-exchange-name]}]
  (queue/bind
    consumer-queue
    {:exchange     retry-exchange-name
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
  [{:keys [backoff-exchange-name backoff]} {:keys [body-raw] :as message}]
  (let [expiration (calculate-backoff backoff message)]
    (-> message
        (assoc :body body-raw, :exchange backoff-exchange-name)
        (assoc-in [:properties :expiration] (str expiration))
        (update-in [:properties :headers] dissoc :x-death)
        (update-in [:properties :headers :x-kithara-retries] (fnil inc 0)))))

(defn- publish-dead-message!
  [{:keys [dead-letter-queue] :as component} message]
  (->> message
       (prepare-dead-message component)
       (publisher/publish dead-letter-queue)))

(defn- normalize-retried-message
  [message]
  (if (not= (get-in message [:properties :headers :x-kithara-retries] ::none)
            ::none)
    (-> message
        (update-in [:properties :headers] dissoc :x-death)
        (assoc :redelivered? true))
    message))

(defn- normalize-result
  [result]
  (or result
      (log/warn
        "[kithara] when using dead letter backoff the handler function"
        "returning `nil` will be interpreted as ACK.")
      {:ack? true}))

(defn- handle-with-backoff
  [component handler message]
  (let [message' (normalize-retried-message message)
        result (-> message' handler normalize-result)
        {:keys [done? nack? reject?]} result
        requeue? (get result :requeue? nack?)]
    (if-not done?
      (if (and (or nack? reject?) requeue?)
        (do
          (publish-dead-message! component message)
          (assoc result :requeue? false))
        result)
      result)))

(defn- prepare-components
  [{:keys [components] :as component}]
  (p/wrap-middleware
    components
    (fn [handler]
      #(handle-with-backoff component handler %))))

;; ## Component

(defcomponent DLXConsumer [components
                           consumer-queue
                           backoff-exchange
                           retry-exchange
                           queue]
  :this/as               *this*
  :backoff-exchange-name (make-backoff-exchange-name *this*)
  :retry-exchange-name   (make-retry-exchange-name *this*)
  :queue-name            (make-dead-letter-queue-name *this*)
  :dead-letter-output    (declare-retry-exchange! *this*)
  :dead-letter-input     (declare-backoff-exchange! *this*)
  :dead-letter-queue     (declare-queue! *this*)
  :components/running    (prepare-components *this*)

  :on/started (bind-queues! *this*)

  p/Wrapper
  (wrap-components [this pred wrap-fn]
    (update this :components p/wrap-components pred wrap-fn))

  p/HasQueue
  (set-queue [this queue]
    (-> this
        (assoc :consumer-queue queue)
        (update :components p/wrap-queue queue))))

(p/hide-constructors DLXConsumer)

;; ## Option Helpers

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

;; ## Wrapper

(defn with-dead-letter-backoff
  "Wrap the given component(s) with setup of dead letter queues and exchanges.
   The following options can be given:

   - `:queue`: options for the dead letter queue (including `:queue-name`,
     `:durable?`, `:exclusive?` and `:auto-delete?`),
   - `:backoff-exchange`: options for the exchange the dead letter queue will be
     bound to (including `:exchange-name`, `:durable?`, `:exclusive?` and
     `:auto-delete?`),
   - `:retry-exchange`: options for the exchange that messages-to-retry will
     be published to (see `:backoff-exchange` for options).

   All three values can be strings, in which case they will be used for
   queue/exchange names and result in a non-durable setup. If no names
   are given, they will be derived from the consumer queue's. Example:

   ```
   (defonce rabbitmq-consumer
     (-> ...
         (with-dead-letter-backoff
           {:queue            \"dead-letters\"
            :backoff-exchange \"dead-letters-backoff\"
            :retry-exchange   \"dead-letters-retry\"})
         (kithara.core/with-queue
           \"consumer-queue\"
           ...)
         ...))
   ```

   __Topology__

   The following is a minimal example, auto-generating names for
   exchanges/queues:

   ```
   (require '[kithara.core :as kithara])

   (defonce rabbitmq-consumer
     (-> (kithara/consumer ...)
         (with-dead-letter-backoff)
         (kithara/with-queue
           \"consumer-queue\"
           {:exchange \"consumer-exchange\", :routing-keys [\"#\"]})
         (kithara/with-channel)
         (kithara/with-connection)))
   ```

   This will create/expect the following exchanges/queues:

   | ------------------------------ |:--------:| --- |
   | `consumer-exchange`            | exchange | has to exist |
   | `consumer-queue--retry`        | exchange | fanout exchange |
   | `consumer-queue--backoff`      | exchange | fanout exchange |
   | `consumer-queue--dead-letters` | queue    | `x-dead-letter-exchange: <retry exchange>` |

   And the following bindings:

   | ------------------------------ | --- |
   | `consumer-queue`               | - `#` via `consumer-exchange`<br />- all from `consumer-queue--retry` (fanout) |
   | `consumer-queue--dead-letters` | all from `consumer-queue--backoff` (fanout) |

   __Behaviour__

   If a consumer NACKs or REJECTs a message with the `:requeue?` flag set
   (default on NACK), or message processing throws an exception, the message
   will be dead-lettered. This means:

   1. Publish the message to the backoff exchange, setting an \"expiration\"
      value. The message will be added to the dead-letter queue.
   2. Once the timeout expires, the message will be published to the retry
      exchange. Since a binding to this exchange was added to the original
      consumer queue, the message will reappear there.

   __Caveat__

   There is one caveat (also noted in the RabbitMQ documentation), consisting of
   expiry only happening at the head of the queue. This means that all messages
   in the dead letter queue will take at least the same time as the current head
   to be republished.

   See: https://www.rabbitmq.com/ttl.html"
  [components & [{:keys [backoff-exchange retry-exchange queue]}]]
  (map->DLXConsumer
    {:components       (p/consumer-seq components)
     :backoff-exchange (as-exchange-map backoff-exchange)
     :retry-exchange   (as-exchange-map retry-exchange)
     :queue            (as-queue-map queue)}))

(defn with-durable-dead-letter-backoff
  "See [[with-dead-letter-backoff]]. Will create/expect durable, non-exclusive and
   non-auto-delete dead-letter queues/exchanges.

   Note that this makes only sense if the original consumer queue has the same
   properties, since otherwise you'll lose dead-lettered messages on retry."
  [components & [{:keys [backoff-exchange retry-exchange queue]}]]
  (let [durify #(merge % {:durable? true, :exclusive? false, :auto-delete? false})]
    (map->DLXConsumer
      {:components       (p/consumer-seq components)
       :backoff-exchange (durify (as-exchange-map backoff-exchange))
       :retry-exchange   (durify (as-exchange-map retry-exchange))
       :queue            (durify (as-queue-map queue))})))
