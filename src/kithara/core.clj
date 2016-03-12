(ns kithara.core
  (:require [kithara
             [base :as base]
             [channels :as channels]
             [connections :as connections]
             [queues :as queues]]
            [potemkin :refer [import-vars]]))

(import-vars
  [kithara.base        consumer]
  [kithara.channels    with-channel]
  [kithara.connections with-connection]
  [kithara.queues      with-queue])
