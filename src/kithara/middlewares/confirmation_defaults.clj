(ns kithara.middlewares.confirmation-defaults)

(def ^:private confirmation-keys
  [:ack? :nack? :reject? :done? :error?])

(defn wrap-confirmation-defaults
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
