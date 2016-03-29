(ns kithara.middlewares.confirmation
  (:require [kithara.rabbitmq.message :as message]
            [manifold.deferred :as d]))

(defn- unknown-status!
  [confirmation message]
  (message/nack message)
  {:status  :error
   :message (str "invalid confirmation:" (pr-str confirmation)) })

(defn- confirm-message!
  [message confirmation]
  (let [message (cond-> message
                  (contains? confirmation :requeue?)
                  (assoc :requeue? (:requeue? confirmation)))]
    (case (:status confirmation)
      :done   nil
      :ack    (message/ack message)
      :nack   (message/nack message)
      :error  (message/nack message)
      :reject (message/reject message)
      (unknown-status! confirmation message))
    confirmation))

(defn- confirm-error
  [t]
  {:status  :error
   :message "when confirming message."
   :error   t})

(defn wrap-confirmation
  "Wrap the given function, taking a kithara message map and producing a
   confirmation map, to ACK/NACK/REJECT the original message based on keys
   contained within. `:status` can have any of the following values:

   - `:ack`
   - `:nack`
   - `:reject`
   - `:error` (an uncaught exception occured)
   - `:done` (do nothing since the message was already explicitly handled)

   For `:nack`, `:reject` and `:error`, an additional key `:requeue?` can
   be given to indicate whether or not the message should be requeued.

   This is a middleware activated by default in the kithara base consumer."
  [message-handler _]
  (fn [message]
    (try
      (-> (message-handler message)
          (d/chain #(confirm-message! message %))
          (d/catch confirm-error))
      (catch Throwable t
        (confirm-error t)))))
