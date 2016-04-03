(ns kithara.test.handler
  (:require [kithara.test.fixtures :as fix])
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
  [message-tracker]
  (let [state (atom {})]
    (fn [{:keys [routing-key body-raw]}]
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
              (doseq [[routing-key data] @message-tracker]
                (assert
                  (.await
                    ^CountDownLatch (:countdown-latch data)
                    wait-ms
                    TimeUnit/MILLISECONDS)
                  (format "message '%s' was not consumed within %dms: %s"
                          routing-key
                          wait-ms
                          (pr-str data)))))]
    (assert (not= ::timeout (deref fut (long (* 1.5 wait-ms)) ::timeout))
            (format "test messages failed to be consumed within %dms."
                    wait-ms))
    true))

(defn verify-expectations!
  [message-tracker]
  (->> (for [[routing-key {:keys [expected-confirmations confirmations]}]
             @message-tracker]
         (= expected-confirmations confirmations))
       (every? true?)))

;; ### Message

(defn make-message
  [expected-confirmations]
  (zipmap (range) expected-confirmations))
