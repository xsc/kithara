(ns kithara.middlewares.logging
  (:require [clojure.tools.logging :as log]))

(defn- make-log-tag
  [consumer-name result {:keys [consumer-tag]}]
  (str
    "[" consumer-name "]"
    (when consumer-tag
      (str " [" consumer-tag "]"))
    (some->> (condp #(get %2 %1) result
               :done?   "[done]"
               :ack?    "[ack]"
               :nack?   "[nack]"
               :reject? "[reject]"
               :error?  "[error]"
               nil)
             (str " "))))

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
          (log/debug tag info))))

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
      (let [result (message-handler message)]
        (write-logs result consumer-name message)
        result)
      (catch Throwable t
        (log/errorf t "uncaught error in message handler.")
        {:error? true, :error t}))))
