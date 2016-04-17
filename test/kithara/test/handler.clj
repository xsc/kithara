(ns kithara.test.handler
  (:require [kithara.test.fixtures :as fix]
            [clojure.tools.logging :as log])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

;; ## Concept
;;
;; The test handler expects an EDN body representing a map associating
;; the number of times a message has been seen with the handler confirmation
;; map, e.g.:
;;
;; ```
;; {0 {:status :nack}
;;  1 {:status :nack}
;;  2 {:status :reject}}
;; ```
;;
;; This represents a message that gets NACKed twice before finally being
;; REJECTed.
;;
;; Additionally, the handler will use a message tracker atom to count
;; down a `CountDownLatch` and store the results.

;; ## Implementation

;; ### Handler

(defn make
  [message-tracker verifiers]
  (let [state (atom {})]
    (fn [{:keys [routing-key body-raw] :as message}]
      (doseq [[verifier-key f] verifiers]
        (when-not (f message)
          (swap! message-tracker
                 update-in [::unverified verifier-key]
                 conj message)))
      (let [seen->confirmation (-> body-raw (String. "UTF-8") read-string)
            seen-before (-> state
                            (swap! update routing-key (fnil inc -1))
                            (get routing-key))
            confirmation (seen->confirmation seen-before)
            tracker' (swap! message-tracker
                            (fn [tracker]
                              (if (contains? tracker routing-key)
                                (update-in tracker
                                           [routing-key :confirmations]
                                           conj
                                           confirmation)
                                tracker)))]
        (some-> tracker'
                ^CountDownLatch (get-in [routing-key :countdown-latch])
                (.countDown))
        confirmation))))

;; ### Tracker

(defn make-tracker
  []
  (atom {}))

(defn track!
  [message-tracker routing-key expected-confirmations]
  (swap! message-tracker
         assoc
         routing-key
         {:confirmations          []
          :expected-confirmations expected-confirmations
          :countdown-latch        (CountDownLatch. (count expected-confirmations))}))

(defn wait-for-messages!
  [message-tracker wait-ms]
  (Thread/sleep (quot wait-ms 10))
  (let [fut (future
              (->> (for [[routing-key data] (dissoc @message-tracker ::unverified)]
                     (or (.await
                           ^CountDownLatch (:countdown-latch data)
                           wait-ms
                           TimeUnit/MILLISECONDS)
                         (log/warnf "message '%s' was not consumed within %dms: %s"
                                    routing-key
                                    wait-ms
                                    (pr-str data))))
                   (every? true?)))]
    (deref fut (long (* 1.5 wait-ms)) nil)))

(defn- verify-messages!
  [message-tracker]
  (let [unverified (::unverified @message-tracker)]
    (or (empty? unverified)
        (doseq [[verifier-key messages] unverified]
          (log/warnf "%d messages failed verification '%s', first: %s"
                     (count messages)
                     verifier-key
                     (pr-str (first messages)))))))

(defn verify-expectations!
  [message-tracker]
  (and (verify-messages! message-tracker)
       (->> (for [[routing-key {:keys [expected-confirmations confirmations]}]
                  (dissoc @message-tracker ::unverified)]
              (= expected-confirmations confirmations))
            (every? true?))))

;; ### Message

(defn make-message
  [expected-confirmations]
  (zipmap (range) expected-confirmations))
