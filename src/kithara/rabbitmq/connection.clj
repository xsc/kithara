(ns kithara.rabbitmq.connection
  (:require [kithara.config :as config])
  (:import [net.jodah.lyra Connections ConnectionOptions]
           [net.jodah.lyra.config Config]
           [com.rabbitmq.client Connection]))

(defn open
  "Create a new RabbitMQ connection backed by Lyra's recovery facilities.
   See `kithara.config/connection` and `kithara.config/behaviour` for available
   options."
  (^com.rabbitmq.client.Connection
    [options]
    (open
      (config/connection options)
      (config/behaviour options)))
  (^com.rabbitmq.client.Connection
    [^ConnectionOptions connection-options ^Config config]
    (Connections/create connection-options config)))

(defn close
  "Close a RabbitMQ connection."
  [^Connection c]
  (.close c))
