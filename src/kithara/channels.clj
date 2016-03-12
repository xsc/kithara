(ns kithara.channels
  (:require [kithara.rabbitmq.channel :as channel]
            [kithara.infrastructure :as i]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- make-consumers
  [{:keys [consumers channel]}]
  (map #(i/set-channel % channel) consumers))

(defcomponent ChannelConsumer [consumers
                               connection
                               channel-number
                               prefetch-count
                               prefetch-size
                               prefetch-global?]
  :assert/connection? (some? connection)
  :this/as            *this*
  :channel            (channel/open connection *this*) #(channel/close %)
  :components/running (make-consumers *this*)

  i/HasConnection
  (set-connection [this connection]
    (assoc this :connection connection)))

;; ## Wrapper

(defn with-channel
  ([consumers] (with-channel consumers {}))
  ([consumers channel-options]
   (map->ChannelConsumer
     (merge
       {:consumers (if (sequential? consumers) consumers [consumers])}
       channel-options))))
