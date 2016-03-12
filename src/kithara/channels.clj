(ns kithara.channels
  (:require [kithara.rabbitmq.channel :as channel]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- make-consumers
  [{:keys [consumers channel]}]
  (map #(assoc % :channel channel) consumers))

(defcomponent ChannelConsumer [consumers
                               connection
                               channel-number
                               prefetch-count
                               prefetch-size
                               prefetch-global?]
  :assert/connection? (some? connection)
  :this/as            *this*
  :channel            (channel/open connection *this*) #(channel/close %)
  :components/running (make-consumers *this*))

;; ## Wrapper

(defn with-channel
  ([consumers] (with-channel consumers {}))
  ([consumers channel-options]
   (map->ChannelConsumer
     (merge
       {:consumers (if (sequential? consumers) consumers [consumers])}
       channel-options))))
