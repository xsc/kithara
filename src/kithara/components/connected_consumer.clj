(ns kithara.components.connected-consumer
  "Implementation of Connection setup/teardown. Please use via `kithara.core`."
  (:require [kithara.rabbitmq
             [connection :as connection]
             [channel :as channel]
             [publish :as publisher]]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- valid-consumers?
  [consumers]
  (and (every? p/has-handler? consumers)
       (every? p/has-connection? consumers)))

(defn- make-consumers
  [{:keys [consumers connection]}]
  (p/set-connection consumers connection))

(defn- open-connection
  [data]
  (connection/open
    (if (contains? data :password)
      (update data :password p/reveal)
      data)))

(defn- close-connection
  [conn]
  (connection/close conn)
  nil)

(defcomponent ConnectedConsumer [consumers]
  :this/as            *this*
  :assert/valid?      (valid-consumers? consumers)
  :connection         (open-connection *this*) #(close-connection %)
  :components/running (make-consumers *this*)

  p/HasHandler
  (wrap-handler [this wrap-fn]
    (update this :consumers p/wrap-handler wrap-fn)))

(p/hide-constructors ConnectedConsumer)

;; ## Wrapper

(defn- prepare-connection-options
  "Hide sensitive information in the connection options."
  [opts]
  (if (contains? opts :password)
    (update opts :password p/hide)
    opts))

(defn with-connection
  "Wrap the given consumer(s) with connection setup/teardown. Accepts
   all options supported by [[kithara.config/connection]] and
   [[kithara.config/behaviour]].

   ```
   (defonce rabbitmq-consumer
     (with-connection
       ...
       {:host     \"rabbitmq.host.com\"
        :vhost    \"/internal\"
        :username \"kithara\"
        :password \"i-am-secret\"}))
   ```

   Note: Consumers have to implement [[HasHandler]] and [[HasConnection]]."
  ([consumers] (with-connection consumers {}))
  ([consumers connection-options]
   {:pre [(valid-consumers? consumers)]}
   (map->ConnectedConsumer
     (merge
       {:consumers       (p/consumer-seq consumers)
        :recovery-policy {:backoff {:max 60000}}
        :retry-policy    :always}
       (prepare-connection-options connection-options)))))
