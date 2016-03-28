(ns kithara.components.queue-wrapper
  "Implementation of Queue setup/teardown. Please use via `kithara.core`."
  (:require [kithara.rabbitmq
             [queue :as queue]
             [channel :as channel]]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Logic

(defn- declare-queue
  [{:keys [declare-channel queue-name declare-queue?] :as opts}]
  (if declare-queue?
    (if queue-name
      (let [opts' (assoc opts :arguments (:declare-arguments opts))]
        (queue/declare declare-channel queue-name opts'))
      (queue/declare declare-channel))
    (queue/declare-passive declare-channel queue-name)))

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

(defn- prepare-components
  [{:keys [components queue]}]
  (p/wrap-queue components queue))

(defn- open-channel
  [{:keys [connection]}]
  (channel/open connection))

(defn- close-channel
  [ch]
  (channel/close ch)
  nil)

;; ## Component

(defcomponent QueueWrapper [components
                            connection
                            queue-name
                            bindings
                            declare-queue?]
  :this/as            *this*
  :assert/connection? (some? connection)
  :declare-channel    (open-channel *this*) #(close-channel %)
  :queue              (make-queue *this*)
  :components/running (prepare-components *this*)

  p/Wrapper
  (wrap-components [this pred wrap-fn]
    (update this :components p/wrap-components pred wrap-fn))

  p/HasConnection
  (set-connection [this connection]
    (-> this
        (assoc :connection connection)
        (update :components p/wrap-connection connection))))

(p/hide-constructors QueueWrapper)

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

   Note: [[set-queue]] will be used to inject the queue."
  ([consumers queue-name]
   (with-queue consumers queue-name {:declare-queue? false}))
  ([consumers queue-name queue-options & more-bindings]
   {:pre [(string? queue-name)]}
   (let [bindings (concat
                    (when (contains? queue-options :exchange)
                      [(select-keys queue-options [:exchange :routing-keys])])
                    more-bindings)]
     (map->QueueWrapper
       (merge
         {:components     (p/consumer-seq consumers)
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

   Note: [[set-queue]] will be used to inject the queue."
  [consumers & bindings]
  (map->QueueWrapper
    {:components    (p/consumer-seq consumers)
     :declare-queue? true
     :bindings       bindings}))
