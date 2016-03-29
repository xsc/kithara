(ns kithara.middlewares.executor
  (:require [manifold.deferred :as d])
  (:import [java.util.concurrent ExecutorService]))

(defn- run-deferred!
  [^ExecutorService e message-handler message]
  (let [d (d/deferred)]
    (->> (fn []
           (try
             (-> (message-handler message)
                 (d/chain #(d/success! d %))
                 (d/catch #(d/error! d %)))
             (catch Throwable t
               (d/error! d t))))
         (.execute e))
    d))

(defn wrap-executor
  "Wrap the given message handler to run on the given `ExecutorService`."
  [message-handler ^ExecutorService e]
  #(run-deferred! e message-handler %))
