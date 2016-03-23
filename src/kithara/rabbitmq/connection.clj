(ns kithara.rabbitmq.connection
  (:require [kithara.config :as config])
  (:import [net.jodah.lyra Connections ConnectionOptions]
           [net.jodah.lyra.config Config]
           [com.rabbitmq.client Connection]
           [com.rabbitmq.client.impl AMQConnection]))

(def ^:private platform
  (let [prop #(System/getProperty (name %))]
    (format "Clojure %s on Java %s (%s)"
            (clojure-version)
            (prop :java.version)
            (prop :java.vm.name))))

(def ^:private client-properties
  (into
    {}
    [(AMQConnection/defaultClientProperties)
     {"product"     "kithara"
      "copyright"   "Copyright (c) 2016 Yannick Scherer"
      "information" "https://github.com/xsc/kithara"
      "platform"    platform
      "version"     "0.1.x"}]))

(defn open
  "Create a new RabbitMQ connection backed by Lyra's recovery facilities.
   See `kithara.config/connection` and `kithara.config/behaviour` for available
   options."
  (^com.rabbitmq.client.Connection
    [options]
    (let [options (update options :properties #(merge client-properties %))]
      (open
        (config/connection options)
        (config/behaviour options))))
  (^com.rabbitmq.client.Connection
    [^ConnectionOptions connection-options ^Config config]
    (Connections/create connection-options config)))

(defn close
  "Close a RabbitMQ connection."
  [^Connection c]
  (.close c))
