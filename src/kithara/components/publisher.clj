(ns kithara.components.publisher
  (:require [kithara.rabbitmq
             [connection :as connection]
             [channel :as channel]
             [publish :as publisher]]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]))

;; ## Component

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

(defcomponent Publisher [options properties]
  :connection (open-connection options) #(close-connection %)
  :channel    (channel/open connection) #(channel/close %)

  p/HasConnection
  (set-connection [this connection]
    (assoc this :connection connection))

  publisher/Publisher
  (publish [_ message]
    (->> (update message :properties #(into properties %))
         (publisher/publish channel)))

  clojure.lang.IFn
  (invoke [this message]
    (publisher/publish this message)))

;; ## Constructor

(defn- prepare-connection-options
  "Hide sensitive information in the connection options."
  [opts]
  (if (contains? opts :password)
    (update opts :password p/hide)
    opts))

(defn publisher
  "Create a publisher component. Accepts all options supported by
   [[kithara.config/connection]] and [[kithara.config/behaviour]].

   `properties` will be used for each message unless explicitly overridden
   (see AMQP's `BasicProperties`).

   The resulting component implements `IFn` and can thus be called like a
   simple function to publish a message."
  [options & [properties]]
  (map->Publisher
    {:options   (prepare-connection-options options)
     :properties properties}))
