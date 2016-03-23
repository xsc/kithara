(ns kithara.components.queue-consumer
  "Implementation of Queue setup/teardown. Please use via `kithara.core`."
  (:require [kithara.rabbitmq.queue :as queue]
            [kithara.components.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Logic

(defn- declare-queue
  [{:keys [channel queue-name declare-queue?] :as opts}]
  (if declare-queue?
    (if queue-name
      (let [opts' (assoc opts :arguments (:declare-arguments opts))]
        (queue/declare channel queue-name opts'))
      (queue/declare channel))
    (queue/declare-passive channel queue-name)))

(defn- bind-queue
  [queue {:keys [bindings declare-queue?]}]
  (when declare-queue?
    (doseq [{:keys [exchange routing-keys arguments]} bindings]
      (queue/bind
        queue
        {:exchange     exchange
         :routing-keys routing-keys
         :arguments    arguments}))))

(defn- make-queue
  [opts]
  (doto (declare-queue opts)
    (bind-queue opts)))

(defn- make-consumers
  [{:keys [consumers queue]}]
  (p/set-queue consumers queue))

;; ## Component

(defn- valid-consumers?
  [consumers]
  (and (every? p/has-handler? consumers)
       (every? p/has-queue? consumers)))

(defcomponent QueueConsumer [consumers
                             channel
                             queue-name
                             bindings
                             declare-queue?]
  :assert/channel?    (some? channel)
  :assert/valid?      (valid-consumers? consumers)
  :this/as            *this*
  :queue              (make-queue *this*)
  :components/running (make-consumers *this*)

  p/HasHandler
  (wrap-handler [this wrap-fn]
    (update this :consumers p/wrap-handler wrap-fn))

  p/HasChannel
  (set-channel [this channel]
    (assoc this :channel channel)))

(p/hide-constructors QueueConsumer)

;; ## Wrapper

(defn with-queue
  "Wrap the given consumer(s) with queue setup/teardown. If options are given,
   the queue will be actively declared using the following keys:

   - `:durable?`
   - `:exclusive?`
   - `:auto-delete?`
   - `:declare-arguments`

   The queue can be bound to an exchange by specifying the following keys in
   `queue-options` or as additional parameter maps:

   - `:exchange`
   - `:routing-keys`
   - `:arguments`

   Example:

   ```
   (defonce rabbitmq-consumer
     (with-queue
       ...
       \"rabbitm-queue\"
       {:exchange \"exchange\", :routing-keys [\"#\"]}
       {:exchange \"other\", :routing-keys [\"*.message\"]}))
   ```

   Note: Consumers have to implement [[HasHandler]] and [[HasQueue]]."
  ([consumers queue-name]
   (with-queue consumers queue-name {:declare-queue? false}))
  ([consumers queue-name queue-options & more-bindings]
   {:pre [(valid-consumers? consumers)
          (string? queue-name)]}
   (let [bindings (concat
                    (when (contains? queue-options :exchange)
                      [(select-keys queue-options [:exchange :routing-keys])])
                    more-bindings)]
     (map->QueueConsumer
       (merge
         {:consumers      (p/consumer-seq consumers)
          :queue-name     queue-name
          :declare-queue? true
          :bindings       bindings}
         (dissoc queue-options :bindings))))))

(defn with-durable-queue
  "See [[with-queue]]. Will create/expect a durable, non-exclusive and
   non-auto-delete queue."
  ([consumers queue-name]
   (with-durable-queue consumers queue-name {:declare-queue? false}))
  ([consumers queue-name queue-options & more-bindings]
   (let [queue-options' (merge
                          queue-options
                          {:durable? true
                           :exclusive? false
                           :auto-delete? false})]
     (apply with-queue consumers queue-name queue-options' more-bindings))))

(defn with-server-named-queue
  "Wrap the given consumer(s) with setup/teardown of a server-named, exclusive,
   non-durable, auto-deleted queue.

   The queue can be bound to an exchange by specifying the following keys in
   as additional parameter maps:

   - `:exchange`
   - `:routing-keys`
   - `:arguments`

   Example:

   ```
   (defonce rabbitmq-consumer
     (with-server-named-queue
       ...
       {:exchange \"exchange\", :routing-keys [\"#\"]}))
   ```

   Note: Consumers have to implement [[HasHandler]] and [[HasQueue]]."
  [consumers & bindings]
  {:pre [(valid-consumers? consumers)]}
  (map->QueueConsumer
    {:consumers      (p/consumer-seq consumers)
     :declare-queue? true
     :bindings       bindings}))
