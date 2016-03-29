(ns kithara.middlewares.confirmation-defaults
  (:require [manifold.deferred :as d]))

(defn- postprocess
  [result default-confirmation error-confirmation]
  (if (map? result)
    (if (keyword? (:status result))
      result
      (merge default-confirmation result))
    default-confirmation))

(defn- confirm-error
  [error-confirmation t]
  (assoc error-confirmation :error t))

(defn wrap-confirmation-defaults
  "Wrap the given function, taking a kithara message map, making sure it
   returns a kithara confirmation map to be processed by [[wrap-confirmation]].

   - `:default-confirmation` will be used if the result is not a map or does not
   contain a valid `:status` key.
   - `:error-confirmation` will be used if an exception is encountered (with
   the exception being `assoc`ed into the map as `:error`).

   This is a middleware activated by default in the kithara base consumer."
  [message-handler {:keys [default-confirmation
                           error-confirmation]
                    :or {default-confirmation {:status :ack}
                         error-confirmation   {:status :nack}}}]
  {:pre [(map? default-confirmation)
         (map? error-confirmation)
         (-> default-confirmation :status keyword?)
         (-> error-confirmation :status keyword?)]}
  (fn [message]
    (try
      (-> (message-handler message)
          (d/chain #(postprocess % default-confirmation error-confirmation))
          (d/catch #(confirm-error error-confirmation %)))
      (catch Throwable t
        (confirm-error error-confirmation t)))))
