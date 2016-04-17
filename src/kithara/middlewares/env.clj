(ns kithara.middlewares.env)

(defn wrap-env
  "Wrap the given message handler to merge (!) the given environment map
   into the `:env` key of each message."
  [message-handler env]
  (fn [message]
    (-> message
        (update :env merge env)
        (message-handler))))
