(ns kithara.components.connection-wrapper
  "Implementation of Connection setup/teardown. Please use via `kithara.core`."
  (:require [kithara.rabbitmq
             [connection :as connection]
             [channel :as channel]
             [publish :as publisher]]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

(defn- prepare-components
  [{:keys [components connection]}]
  (p/wrap-connection components connection))

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

(defcomponent ConnectionWrapper [components]
  :this/as            *this*
  :connection         (open-connection *this*) #(close-connection %)
  :components/running (prepare-components *this*)

  p/Wrapper
  (wrap-components [this pred wrap-fn]
    (update this :components p/wrap-components pred wrap-fn)))

(p/hide-constructors ConnectionWrapper)

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

   Note: `kithara.protocols/set-connection` will be used to inject the
   connection."
  ([consumers] (with-connection consumers {}))
  ([consumers connection-options]
   (map->ConnectionWrapper
     (merge
       {:components      (p/consumer-seq consumers)
        :recovery-policy {:backoff {:max 60000}}
        :retry-policy    :always}
       (prepare-connection-options connection-options)))))
