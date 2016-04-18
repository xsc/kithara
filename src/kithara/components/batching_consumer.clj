(ns kithara.components.batching-consumer
  (:require [kithara.components.base-consumer :as base]
            [kithara.protocols :as p]
            [manifold.deferred :as d]
            [peripheral.core :refer [defcomponent]]
            [clojure.tools.logging :as log])
  (:import [java.util ArrayList]
           [java.util.concurrent
            BlockingQueue
            LinkedBlockingQueue
            Executors
            ExecutorService
            TimeUnit]))

;; ## Per-Message Tasks

(defn- ->task
  [message]
  {:message         message
   :result-deferred (d/deferred)})

(defn- task-failed!
  [{:keys [result-deferred]} t]
  (d/error! result-deferred t))

(defn- tasks-failed!
  [tasks t]
  (doseq [task tasks]
    (task-failed! task t)))

(defn- task-completed!
  [{:keys [result-deferred]} v]
  (d/success! result-deferred v))

(defn- tasks-completed!
  [tasks vs]
  (let [padded (concat vs (repeat nil))]
    (doseq [[task v] (map vector tasks padded)]
      (task-completed! task v))))

(defn- task-timed-out!
  [{:keys [result-deferred]}]
  (d/success!
    result-deferred
    {:status :nack
     :message "message could not be added to batchwise processing queue."}))

;; ## Queue Logic

(defn- fetch-tasks!
  "Fetch a batch of tasks "
  [{:keys [^BlockingQueue queue
           ^ArrayList buffer
           batch-size]}]
  (let [task-count (.drainTo queue buffer batch-size)]
    (when (pos? task-count)
      (let [tasks (into [] buffer)]
        (.clear buffer)
        tasks))))

(defn- wait-for-messages!
  [{:keys [queue interval-ms]}]
  (locking queue
    (try
      (.wait queue interval-ms)
      (catch InterruptedException _))))

(defn- maybe-process-batch!
  [{:keys [^BlockingQueue queue batch-size]}]
  (locking queue
    (when (>= (.size queue) batch-size)
      (.notifyAll queue))))

;; ## Batch Processing

(defn- process-messages!
  [{:keys [batch-handler]} tasks]
  (try
    (let [messages (map :message tasks)
          result   (batch-handler messages)]
      (if (not (sequential? result))
        (tasks-completed! tasks (repeat result))
        (tasks-completed! tasks result)))
    (catch Throwable t
      (tasks-failed! tasks t)))
  tasks)

(defn- handle-batch!
  [component]
  (try
    (some->> (fetch-tasks! component)
             (process-messages! component)
             (seq))
    (catch Throwable t
      (log/errorf t "uncaught exception in batchwise message handling."))))

(defn- drain-queue!
  [component]
  (while (handle-batch! component)))

;; ## Periodic Draining

(defn- run-scheduler-loop!
  [{:keys [shutdown? wait-promise] :as component}]
  (try
    (drain-queue! component)
    (loop []
      (wait-for-messages! component)
      (drain-queue! component)
      (when-not @shutdown?
        (recur)))
    (finally
      (deliver wait-promise nil))))

(defn- start-scheduler
  [component]
  (let [shutdown?    (atom false)
        wait-promise (promise)
        component    (assoc component
                            :shutdown? shutdown?
                            :wait-promise wait-promise)]
    (doto (Thread. #(run-scheduler-loop! component))
      (.setName "kithara-batching-scheduler")
      (.start))
    {:shutdown?    shutdown?
     :wait-promise wait-promise}))

(defn- stop-scheduler
  [{:keys [queue
           shutdown?
           wait-promise]}]
  (reset! shutdown? true)
  (locking queue
    (.notifyAll queue))
  @wait-promise)

;; ## Consumer

(defn- offer-message!
  [message {:keys [^BlockingQueue queue offer-timeout-ms] :as component}]
  (let [task (->task message)]
    (when-not (.offer queue task offer-timeout-ms TimeUnit/MILLISECONDS)
      (task-timed-out! task))
    (maybe-process-batch! component)
    (:result-deferred task)))

(defn- make-consumer
  [{:keys [channel consumer-queue middlewares opts] :as component}]
  (-> #(offer-message! % component)
      (base/consumer opts)
      (p/set-channel channel)
      (p/set-queue consumer-queue)
      (p/add-middleware middlewares)))

;; ## Component

(defcomponent BatchingConsumer [batch-handler
                                batch-size
                                channel
                                consumer-queue
                                middlewares
                                queue-size
                                interval-ms
                                offer-timeout-ms
                                opts]
  :this/as            *this*
  :buffer             (ArrayList. batch-size)
  :queue              (LinkedBlockingQueue. queue-size)
  :scheduler          (start-scheduler *this*) #(stop-scheduler %)
  :component/consumer (make-consumer *this*)

  p/HasChannel
  (set-channel [this channel]
    (assoc this :channel channel))

  p/HasQueue
  (set-queue [this queue]
    (assoc this :consumer-queue queue))

  p/Consumer
  (add-middleware [this wrap-fn]
    (update this :middlewares #(or (some->> % (comp wrap-fn)) wrap-fn))))

(p/hide-constructors BatchingConsumer)

;; ## Constructor

(defn ^{:added "0.1.2"} batching-consumer
  "Generate a consumer component based on the given batch processing function.

   The batch processing function gets a seq of messages and produces either:

   - a single confirmation map for all messages, or
   - a seq of in-order confirmation maps for each individual message.

   If less confirmations are returned than messages were given, `nil` will be
   used for the remaining ones, triggering the default confirmation handling of
   the consumer."
  ([batch-handler]
   (batching-consumer batch-handler {}))
  ([batch-handler
    {:keys [batch-size
            interval-ms
            offer-timeout-ms
            queue-size]
     :or {batch-size       128
          interval-ms      200
          offer-timeout-ms 1000
          queue-size       256}
     :as opts}]
   (map->BatchingConsumer
     {:batch-handler    batch-handler
      :offer-timeout-ms offer-timeout-ms
      :batch-size       batch-size
      :interval-ms      interval-ms
      :queue-size       queue-size
      :opts             (dissoc opts :batch-size :offer-timeout-ms)})))
