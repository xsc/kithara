(ns kithara.core
  "Public API for simple RabbitMQ consumer creation."
  (:require [kithara.components
             [base-consumer :as base-consumer]
             [channel-consumer :as channel-consumer]
             [connected-consumer :as connected-consumer]
             [queue-consumer :as queue-consumer]]
            [potemkin :refer [import-vars]]))

(import-vars
  [kithara.components.base-consumer
   consumer]
  [kithara.components.channel-consumer
   with-channel
   with-prefetch-channel]
  [kithara.components.connected-consumer
   with-connection]
  [kithara.components.queue-consumer
   with-queue
   with-durable-queue
   with-server-named-queue])
