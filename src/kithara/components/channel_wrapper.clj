(ns kithara.components.channel-wrapper
  "Implementation of Channel setup/teardown. Please use via `kithara.core`."
  (:require [kithara.rabbitmq.channel :as channel]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- prepare-components
  [{:keys [components channel]}]
  (p/wrap-channel components channel))

(defcomponent ChannelWrapper [components
                              connection
                              channel-number
                              prefetch-count
                              prefetch-size
                              prefetch-global?]
  :assert/connection? (some? connection)
  :this/as            *this*
  :channel            (channel/open connection *this*) #(channel/close %)
  :components/running (prepare-components *this*)

  p/Wrapper
  (wrap-components [this pred wrap-fn]
    (update this :components p/wrap-components pred wrap-fn))

  p/HasConnection
  (set-connection [this connection]
    (-> this
        (assoc :connection connection)
        (update :components p/wrap-connection connection))))

(p/hide-constructors ChannelWrapper)

;; ## Wrapper

(defn with-channel
  "Wrap the given component(s) with setup/teardown of a RabbitMQ channel. The
   following options can be given:

   - `:channel-number`
   - `:prefetch-count`
   - `:prefetch-size`
   - `:prefetch-global?`

   If no options are given, a channel with server-side default settings will
   be set up.

   Note: [[set-channel]] will be used to inject the channel."
  ([consumers] (with-channel consumers {}))
  ([consumers channel-options]
   (map->ChannelWrapper
     (merge
       {:components (p/consumer-seq consumers)}
       channel-options))))

(defn with-prefetch-channel
  "See [[with-channel]]. Convenience function setting the per-channel prefetch
   count directly."
  [consumers prefetch-count & [channel-options]]
  (->> channel-options
       (merge {:prefetch-count prefetch-count})
       (with-channel consumers)))
