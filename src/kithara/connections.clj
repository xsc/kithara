(ns kithara.connections
  (:require [kithara.rabbitmq
             [connection :as connection]
             [channel :as channel]
             [publish :as publisher]]
            [kithara.infrastructure :as i]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- make-consumers
  [{:keys [consumers connection]}]
  (map
    #(i/set-connection % connection)
    consumers))

(defcomponent ConnectedConsumer [consumers]
  :this/as            *this*
  :connection         (connection/open *this*) #(connection/close %)
  :components/running (make-consumers *this*))

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
