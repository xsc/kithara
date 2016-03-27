(ns kithara.components.base-consumer
  "Implementation of Consumer setup/teardown. Please use via `kithara.core`."
  (:require [kithara.rabbitmq
             [consumer :as consumer]
             [utils :as u]]
            [kithara.middlewares
             [confirmation :refer [wrap-confirmation]]
             [confirmation-defaults :refer [wrap-confirmation-defaults]]
             [logging :refer [wrap-logging]]]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]
            [clojure.tools.logging :as log]))

;; ## Logic

(defn- consumer-tag-for
  [consumer-name]
  (format "%s:%s"
          consumer-name
          (u/random-string)))

(defn- prepare-consumer-tag
  [{:keys [consumer-name opts]}]
  (or (:consumer-tag opts)
      (consumer-tag-for consumer-name)))

(defn- prepare-consumer-options
  [{:keys [queue-name opts] :as component}]
  (let [consumer-tag (prepare-consumer-tag component)]
    (assoc opts
           :queue-name   queue-name
           :consumer-tag consumer-tag
           :auto-ack?    false)))

(defn- prepare-consumer
  ^com.rabbitmq.client.Consumer
  [{:keys [handler middlewares consumer-name channel opts]}]
  (-> handler
      (wrap-confirmation-defaults opts)
      (cond-> middlewares middlewares)
      (wrap-confirmation opts)
      (wrap-logging consumer-name)
      (consumer/from-fn channel opts)))

(defn- log-startup!
  [{:keys [consumer-name consumer-options]}]
  (let [{:keys [queue-name consumer-tag]} consumer-options]
    (log/debugf "[%s] starting consumer on queue %s (desired tag: '%s') ..."
                consumer-name
                queue-name
                consumer-tag)))

(defn- run-consumer!
  [{:keys [channel consumer-impl consumer-options] :as component}]
  (log-startup! component)
  (consumer/consume channel consumer-options consumer-impl))

(defn- stop-consumer!
  [{:keys [consumer-name]} consumer-value]
  (log/debugf "[%s] stopping consumer ..." consumer-name)
  (consumer/cancel consumer-value))

;; ## Component

(defcomponent BaseConsumer [consumer-name
                            queue-name
                            channel
                            middlewares
                            handler
                            opts]
  :this/as             *this*
  :assert/queue-name? (string? queue-name)
  :assert/channel?    (some? channel)
  :consumer-options   (prepare-consumer-options *this*)
  :consumer-impl      (prepare-consumer *this*)
  :consumer           (run-consumer! *this*) #(stop-consumer! *this* %)

  p/HasChannel
  (set-channel [this channel]
    (assoc this :channel channel))

  p/HasQueue
  (set-queue [this queue]
    (assoc this :queue-name (:queue-name queue)))

  p/Consumer
  (add-middleware [this wrap-fn]
    (update this :middlewares  #(or (some->> % (comp wrap-fn)) wrap-fn))))

(p/hide-constructors BaseConsumer)

;; ## Constructor

(defn consumer
  "Create a new kithara `BaseConsumer` using the given handler.

   Options:

   - `:consumer-name`: the consumer's name,
   - `:default-confirmation`: the default confirmation map,
   - `:error-confirmation`: the confirmation map in case of exception,
   - `:as`: the coercer to use for incoming message bodies,
   - `:consumer-tag`,
   - `:local?`,
   - `:exclusive?`,
   - `:arguments`.

   See the documentation of `basic.consume` for an explanation of `:consumer-tag`,
   `:local?`, `:exclusive?` and `:arguments`.

   The following values are valid for `:as`:

   - `:bytes` (default),
   - `:string`
   - any function taking the raw byte array as input,
   - any value implementing [[kithara.protocols/Coercer]].

   The handler function gets a message map with the following keys:

   - `:channel`: the channel the message was received on,
   - `:exchange`: the exchange the message was published to,
   - `:routing-key`: the message's routing key,
   - `:body`: the coerced body,
   - `:body-raw`: the body as a byte array,
   - `:properties`: a map of message properties,
   - `:redelivered?`: whether the message was redelivered,
   - `:delivery-tag`: the message's delivery tag.

   Messages will be confirmed based on the return value of the handler function:

   - `{:reject? true, :requeue? <bool>}` -> REJECT (defaults to no requeue),
   - `{:nack? true, :requeue? <bool>}` -> NACK (defaults to requeue),
   - `{:ack? true}`-> ACK,
   - `{:done? true}` -> do nothing (was handled directly).

   Additionally, `:message` (a string) and `:error` (a `Throwable`) keys can be
   added to trigger a log message. See [[wrap-confirmation]] and
   [[wrap-logging]]."
  ([handler] (consumer handler {}))
  ([handler
    {:keys [consumer-name]
     :or {consumer-name "kithara"}
     :as opts}]
   {:pre [handler]}
   (map->BaseConsumer
     {:consumer-name consumer-name
      :handler       handler
      :opts          (dissoc opts :consumer-name :auto-ack?)})))
