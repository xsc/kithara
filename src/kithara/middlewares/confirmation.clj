(ns kithara.middlewares.confirmation
  (:require [kithara.rabbitmq.message :as message]))

;; ## Data

(def ^:private confirmation-keys
  [:ack? :nack? :reject? :done? :error?])

;; ## Confirmation Map

(defn wrap-confirmation-map
  "Wrap the given function, taking a kithara message map, making sure it
   returns a kithara confirmation map to be processed by [[wrap-confirmation]].

   - `:default-confirmation` will be used if the result is not a map or does not
   contain any of `:ack?`, `:nack?`, `:reject?`, `:done?` or `:error?`.
   - `:error-confirmation` will be used if an exception is encountered (with
   the exception being `assoc`ed into the map as `:error`).

   This is a middleware activated by default in the kithara base consumer."
  [message-handler {:keys [default-confirmation
                           error-confirmation]
                    :or {default-confirmation {:ack? true}
                         error-confirmation   {:nack? true}}}]
  {:pre [(map? default-confirmation) (map? error-confirmation)]}
  (fn [message]
    (try
      (let [result (message-handler message)]
        (if (map? result)
          (if (some #(contains? result %) confirmation-keys)
            result
            (merge default-confirmation result))
          default-confirmation))
      (catch Throwable t
        (assoc error-confirmation :error t)))))

;; ## Confirmation Handling

(defn- confirm-message!
  [message result]
  (let [message (cond-> message
                  (contains? result :requeue?)
                  (assoc :requeue? (:requeue? result)))]
    (condp #(get %2 %1) result
      :done?   nil
      :ack?    (message/ack message)
      :nack?   (message/nack message)
      :error?  (message/nack message)
      :reject? (message/reject message)
      nil)))

(defn wrap-confirmation
  "Wrap the given function, taking a kithara message map and producing a
   confirmation map, to ACK/NACK/REJECT the original message based on keys
   contained within:

   - `{:reject? true, :requeue? <bool>}` -> REJECT (defaults to no requeue),
   - `{:nack? true, :requeue? <bool>}` -> NACK (defaults to requeue),
   - `{:ack? true}`-> ACK,
   - `{:done? true}` -> do nothing (was handled directly).

   This is a middleware activated by default in the kithara base consumer."
  [message-handler _]
  (fn [message]
    (try
      (->> (message-handler message)
           (confirm-message! message))
      (catch Throwable t
        {:error?  true
         :message "when confirming message."
         :error   t}))))
