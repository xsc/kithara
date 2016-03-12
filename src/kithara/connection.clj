(ns kithara.connection
  (:require [kithara.rabbitmq
             [connection :as connection]
             [channel :as channel]
             [publish :as publisher]]
            [kithara.infrastructure :as i]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- make-consumers
  [{:keys [consumers connection channel]}]
  (map
    #(-> %
         (i/set-connection connection)
         (i/set-channel channel))
    consumers))

(defcomponent ConnectedConsumer [consumers]
  :this/as            *this*
  :connection         (connection/open *this*) #(connection/close %)
  :channel            (channel/open connection) #(channel/close %)
  :components/running (make-consumers *this*)

  publisher/Publisher
  (publish [_ message]
    (publisher/publish channel message)))

;; ## Wrapper

(defn with-connection
  ([consumers] (with-connection consumers {}))
  ([consumers connection-options]
   (map->ConnectedConsumer
     (merge
       {:consumers       (if (sequential? consumers) consumers [consumers])
        :recovery-policy {:backoff {:max 60000}}
        :retry-policy    :always}
       connection-options))))
