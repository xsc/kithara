(ns kithara.middlewares.confirmation
  (:require [kithara.rabbitmq.message :as message]))

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
      nil)
    result))

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
