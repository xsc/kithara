(ns kithara.infrastructure
  (:require [potemkin :refer [defprotocol+]]))

;; ## Protccols

(defprotocol+ HasChannel
  (set-channel [_ channel]))

(defprotocol+ HasQueue
  (set-queue [_ queue]))

(defprotocol+ HasConnection
  (set-connection [_ connection]))

(defprotocol+ HasHandler
  (wrap-handler [_ wrap-fn]))

;; ## Do Nothing Unless Implemented

(defmacro ^:private extend-identity
  [& protocols]
  `(do
     ~@(for [[protocol f] protocols]
         `(extend-protocol ~protocol
            Object
            (~f [this# ~'_] this#)))))

(extend-identity
  [HasChannel    set-channel]
  [HasQueue      set-queue]
  [HasHandler    wrap-handler]
  [HasConnection set-connection])
