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
      (cond (and prefetch-size prefetch-global?)
            (.basicQos
              ch
              (int prefetch-count)
              (int prefetch-size)
              (boolean prefetch-global?))
            prefetch-size
            (.basicQos ch (int prefetch-count) (int prefetch-size))
            :else (.basicQos ch (int prefetch-count))))
    ch))

(defn close
  "Close a RabbitMQ channel."
  ([^Channel channel]
   (.close channel))
  ([^Channel channel close-code close-message]
   (.close channel (int close-code) (str close-message))))
