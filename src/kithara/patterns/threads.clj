(ns kithara.patterns.threads
  (:require [kithara.middlewares.executor :refer [wrap-executor]]
            [kithara.protocols :as p]
            [peripheral.core :refer [defcomponent]]
            [clojure.tools.logging :refer [errorf]])
  (:import [java.util.concurrent Executors ExecutorService TimeUnit]))

;; ## Logic

(defn- start-executor
  [{:keys [thread-count]}]
  (Executors/newFixedThreadPool thread-count))

(defn- stop-executor
  [^ExecutorService e]
  (try
    (.shutdown e)
    (when-not (.awaitTermination e 3 TimeUnit/SECONDS)
      (.shutdownNow e)
      (when-not (.awaitTermination e 3 TimeUnit/SECONDS)
        (errorf "thread pool did not shut down:" e)))
    (catch InterruptedException _
      (.shutdownNow e)
      (.interrupt (Thread/currentThread)))))

(defn- prepare-components
  [{:keys [components executor]}]
  (p/wrap-middleware components #(wrap-executor % executor)))

;; ## Component

(defcomponent ThreadWrapper [components thread-count]
  :this/as            *this*
  :executor           (start-executor *this*) #(stop-executor %)
  :components/running (prepare-components *this*)

  p/Wrapper
  (wrap-components [this pred wrap-fn]
    (update this :components p/wrap-components pred wrap-fn)))

(p/hide-constructors ThreadWrapper)

;; ## Wrapper

(defn with-threads
  "Let message handling be run in a fixed-size thread pool."
  [components thread-count]
  {:pre [(pos? thread-count)]}
  (map->ThreadWrapper
    {:components   (p/consumer-seq components)
     :thread-count thread-count}))
