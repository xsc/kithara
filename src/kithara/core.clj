(ns kithara.core
  "Public API for simple RabbitMQ consumer creation."
  (:require [kithara.components
             base-consumer
             batching-consumer
             channel-wrapper
             connection-wrapper
             env-wrapper
             publisher
             queue-wrapper]
            [potemkin :refer [import-vars]]))

(import-vars
  [kithara.components.base-consumer
   consumer]
  [kithara.components.batching-consumer
   batching-consumer]
  [kithara.components.channel-wrapper
   with-channel
   with-prefetch-channel]
  [kithara.components.connection-wrapper
   with-connection]
  [kithara.components.env-wrapper
   with-env]
  [kithara.components.publisher
   publisher]
  [kithara.components.queue-wrapper
   with-queue
   with-durable-queue
   with-server-named-queue])
