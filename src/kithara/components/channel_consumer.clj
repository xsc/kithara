(ns kithara.components.channel-consumer
  "Implementation of Channel setup/teardown. Please use via `kithara.core`."
  (:require [kithara.rabbitmq.channel :as channel]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- valid-consumers?
  [consumers]
  (and (every? p/has-handler? consumers)
       (every? p/has-channel? consumers)))

(defn- make-consumers
  [{:keys [consumers channel]}]
  (p/set-channel consumers channel))

(defcomponent ChannelConsumer [consumers
                               connection
                               channel-number
                               prefetch-count
                               prefetch-size
                               prefetch-global?]
  :assert/connection? (some? connection)
  :assert/valid?      (valid-consumers? consumers)
  :this/as            *this*
  :channel            (channel/open connection *this*) #(channel/close %)
  :components/running (make-consumers *this*)

 p/HasHandler
  (wrap-handler [this wrap-fn]
    (update this :consumers p/wrap-handler wrap-fn))

  p/HasConnection
  (set-connection [this connection]
    (assoc this :connection connection)))

(p/hide-constructors ChannelConsumer)

;; ## Wrapper

(defn with-channel
  "Wrap the given consumer(s) with setup/teardown of a RabbitMQ channel. The
   following options can be given:

   - `:channel-number`
   - `:prefetch-count`
   - `:prefetch-size`
   - `:prefetch-global?`

   If no options are given, a channel with server-side default settings will
   be set up.

   Note: Consumers have to implement [[HasHandler]] and [[HasChannel]]."
  ([consumers] (with-channel consumers {}))
  ([consumers channel-options]
   {:pre [(valid-consumers? consumers)]}
   (map->ChannelConsumer
     (merge
       {:consumers (p/consumer-seq consumers)}
       channel-options))))

(defn with-prefetch-channel
  "See [[with-channel]]. Convenience function setting the per-channel prefetch
   count directly."
  [consumers prefetch-count & [channel-options]]
  (->> channel-options
       (merge {:prefetch-count prefetch-count})
       (with-channel consumers)))
