(ns kithara.core
  (:require [kithara
             [base :as base]
             [channels :as channels]
             [connection :as connection]
             [queues :as queues]]
            [potemkin :refer [import-vars]]))

(import-vars
  [kithara.base       consumer]
  [kithara.channels   with-channel]
  [kithara.connection with-connection]
  [kithara.queues     with-queue])
