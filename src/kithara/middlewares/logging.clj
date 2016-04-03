(ns kithara.middlewares.logging
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as d]))

(defn- make-log-tag
  [consumer-name result {:keys [consumer-tag]}]
  (str
    "[" consumer-name "]"
    (when consumer-tag
      (str " [" consumer-tag "]"))
    (if-let [status (:status result)]
      (if (contains? result :requeue?)
        (format " [%s] <%s>"
                (name status)
                (if (:requeue? result)
                  "requeue"
                  "drop"))
        (format " [%s]" (name status))))))

(defn- make-log-info
  [{:keys [exchange routing-key body-raw]}]
  (format "exchange=%s, routing-key=%s, size=%d"
          (pr-str exchange)
          (pr-str routing-key)
          (alength ^bytes body-raw)))

(defn- write-logs
  [{:keys [message error] :as result} consumer-name message-data]
  (let [tag (make-log-tag consumer-name result message-data)
        info (make-log-info message-data)]
    (cond (instance? Throwable error)
          (log/errorf error
                      "%s %s (%s)"
                      tag
                      (or message "an exception occured.")
                      info)
          (some? error)
          (log/errorf "%s %s - %s (%s)"
                      tag
                      (or message "an exception occured.")
                      error
                      info)
          (some? message)
          (log/debugf "%s %s (%s)" tag message info)
          :else
          (log/debug tag info)))
  result)

(defn- handle-error
  [t]
  (log/errorf t "uncaught error in message handler.")
  {:status :error, :error t})

(defn wrap-logging
  "Wrap the given function, taking a kithara message map and producing a
   confirmation map, to log messages on error/exceptions. The message
   will have the following format:

   ```
   [<consumer-name>] [<consumer-tag>] [<status>] ... details ...
   ```

   This is a middleware activated by default in the kithara base consumer."
  [message-handler consumer-name]
  (fn [message]
    (try
      (-> (message-handler message)
          (d/chain #(write-logs % consumer-name message))
          (d/catch handle-error))
      (catch Throwable t
        (handle-error t)))))
