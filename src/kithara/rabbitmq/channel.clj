(ns kithara.rabbitmq.channel
  (:import [com.rabbitmq.client Connection Channel]))

(defn open
  "Open a new RabbitMQ channel with the given channel number and prefetch
   values."
  ^com.rabbitmq.client.Channel
  [^Connection c &
   [{:keys [channel-number
            prefetch-count
            prefetch-size
            prefetch-global?]}]]
  (when-let [ch (if channel-number
                  (.createChannel c (int channel-number))
                  (.createChannel c))]
    (when prefetch-count
      (cond (and prefetch-size (some? prefetch-global?))
            (.basicQos
              ch
              (int prefetch-size)
              (int prefetch-count)
              (boolean prefetch-global?))
            (some? prefetch-global?)
            (.basicQos ch (int prefetch-count) (boolean prefetch-global?))
            :else (.basicQos ch (int prefetch-count))))
    ch))

(defn close
  "Close a RabbitMQ channel."
  ([^Channel channel]
   (.close channel))
  ([^Channel channel close-code close-message]
   (.close channel (int close-code) (str close-message))))
