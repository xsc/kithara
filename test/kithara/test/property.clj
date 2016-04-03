(ns kithara.test.property
  (:require [clojure.test.check
             [generators :as gen]
             [properties :as prop]]
            [kithara.test
             [confirmations :as confirmations]
             [fixtures :as fix]
             [handler :as handler]]
            [kithara.utils :refer [random-string]]
            [peripheral.core :as p]))

;; ## Publish Logic

(defn- publish!
  [message-tracker confirmations]
  (let [routing-key (random-string)
        body (-> confirmations
                 (handler/make-message)
                 (pr-str)
                 (.getBytes "UTF-8"))]
    (handler/track! message-tracker routing-key confirmations)
    (fix/publish! routing-key body)))

;; ## Property

(defn consumer-property
  ([stack-build-fn] (consumer-property {} stack-build-fn))
  ([{:keys [message-count wait-ms]
     :or {message-count 5
          wait-ms 2000}}
    stack-build-fn]
   {:pre [(pos? message-count)
          (pos? wait-ms)]}
   (prop/for-all
     [confirmations (gen/vector confirmations/gen 1 message-count)]
     (let [message-tracker (handler/make-tracker)
           message-handler (handler/make message-tracker)]
       (p/with-start [_ (stack-build-fn
                          message-handler
                          (fix/connection-config)
                          (fix/exchange-name))]
         (doseq [sq confirmations]
           (publish! message-tracker sq))
         (and (handler/wait-for-messages! message-tracker wait-ms)
              (handler/verify-expectations! message-tracker)))))))
