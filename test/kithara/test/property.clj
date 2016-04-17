(ns kithara.test.property
  (:require [clojure.test.check
             [generators :as gen]
             [properties :as prop]]
            [kithara.test
             [confirmations :as confirmations]
             [fixtures :as fix]
             [handler :as handler]]
            [kithara.utils :refer [random-string]]
            [clojure.pprint :as pprint]
            [clojure.tools.logging :as log]
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

(defn- log-stack!
  [forms]
  (binding [pprint/*print-pprint-dispatch* pprint/code-dispatch]
    (->> forms
         (concat '[-> message-handler])
         (pprint/pprint)
         ^String (with-out-str)
         (.trim)
         (log/debugf "[stack under test]%n%s"))))

(defn consumer-property
  ([stack-gen] (consumer-property {} stack-gen))
  ([{:keys [message-count wait-ms]
     :or {message-count 5
          wait-ms 2000}}
    stack-gen]
   {:pre [(pos? message-count)
          (pos? wait-ms)]}
   (prop/for-all
     [confirmations (gen/vector confirmations/gen 1 message-count)
      {:keys [forms verifiers build-fn]} stack-gen]
     (log-stack! forms)
     (let [message-tracker (handler/make-tracker)
           message-handler (handler/make message-tracker verifiers)
           stack (build-fn
                   message-handler
                   {:connection (fix/connection-config)
                    :queue      (random-string)
                    :exchange   (fix/exchange-name)})]
       (p/with-start [_ stack]
         (doseq [sq confirmations]
           (publish! message-tracker sq))
         (and (handler/wait-for-messages! message-tracker wait-ms)
              (handler/verify-expectations! message-tracker)))))))
