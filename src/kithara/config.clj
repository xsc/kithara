(ns kithara.config
  (:import [net.jodah.lyra.config
            RecoveryPolicies
            RetryPolicies]
           [net.jodah.lyra.event
            ConnectionListener
            ChannelListener
            ConsumerListener]
           [net.jodah.lyra.util Duration]))


;; ## Helper Macro

(defn- generate-builder
  [result-class method]
  (if (sequential? method)
    (let [[method-name _ & processors] method
          this       (with-meta 'this {:tag result-class})
          param      (gensym)]
      `(fn [~this ~param]
         (. ~this ~method-name
            ~@(for [processor processors]
                (if (= processor ::none)
                  param
                  `(~processor ~param))))))
    (recur result-class [method])))

(defn- run-builders
  [builders initial config]
  (reduce
    (fn [result [k v]]
      (if-let [builder (get builders k)]
        (builder result v)
        result))
    initial config))

(defn- generate-meta
  [result-class mapping]
  {:arglists '([] [configuration-map] [base configuration-map])
   :tag      result-class
   :doc      (apply str
                    "Generates a `" result-class "` (see Lyra documentation).\n\n"
                    (for [[k [f]] (sort-by key mapping)]
                      (str "    - `" k "`: delegates to `" f "`.\n")))})

(defmacro ^:private defconfig
  "Creates a function that converts a value (e.g. a Clojure map) into a
   configuration object."
  [sym _ result-class mapping & [defaults]]
  `(let [builders# ~(->> (for [[k method] mapping]
                           [k (generate-builder result-class method)])
                         (into {}))
         defaults# ~defaults]
     (doto (defn ~sym
             ([] (~sym {}))
             ([config#]
              (if (instance? ~result-class config#)
                config#
                (or (some-> (get defaults# config#) (~sym))
                    (~sym (new ~result-class) config#))))
             ([initial# config#]
              (run-builders builders# initial# config#)))
       (alter-meta!
         merge
         (quote ~(generate-meta result-class mapping))))))

;; ## Converters

(defn- to-int
  ([value] (int value))
  ([k default]
   #(to-int (get % k default))))

(defn- to-duration
  ([value]
   (if (instance? Duration value)
     value
     (Duration/milliseconds value)))
  ([k default]
   (fn [value]
     (let [n (get value k default)]
       (to-duration n)))))

(defn- varargs
  [class]
  #(into-array class %))

(defn with
  [f mapping]
  (fn [value]
    (f (get mapping value value))))

;; ## RecoveryPolicy

(defconfig recovery-policy :> net.jodah.lyra.config.RecoveryPolicy
  {:backoff
   [withBackoff :<
    (to-duration :min 1000)
    (to-duration :max 30000)
    (to-int      :factor 2)]
   :interval
   [withInterval :< to-duration]
   :max-attempts
   [withMaxAttempts :< (with to-int {:always -1})]
   :max-duration
   [withMaxDuration :< to-duration]}
  {:always (RecoveryPolicies/recoverAlways)
   :never  (RecoveryPolicies/recoverNever)})

;; ## Retry Policy

(defconfig retry-policy :> net.jodah.lyra.config.RetryPolicy
  {:backoff
   [withBackoff :<
    (to-duration :min 1000)
    (to-duration :max 30000)
    (to-int      :factor 2)]
   :interval
   [withInterval :< to-duration]
   :max-attempts
   [withMaxAttempts :< (with to-int {:always -1})]
   :max-duration
   [withMaxDuration :< to-duration]}
  {:always (RetryPolicies/retryAlways)
   :never  (RetryPolicies/retryNever)})

;; ## Top-Level Config

(defconfig build :> net.jodah.lyra.config.Config
  {:recovery-policy      [withRecoveryPolicy      :< recovery-policy]
   :retry-policy         [withRetryPolicy         :< retry-policy]
   :queue-recovery?      [withQueueRecovery       :< boolean]
   :exchange-recovery?   [withExchangeRecovery    :< boolean]
   :consumer-recovery?   [withConsumerRecovery    :< boolean]
   :connection-listeners [withConnectionListeners :< (varargs ConnectionListener)]
   :channel-listeners    [withChannelListeners    :< (varargs ChannelListener)]
   :consumer-listeners   [withConsumerListener    :< (varargs ConsumerListener)]})

;; ## Connection Options

(defconfig connection :> net.jodah.lyra.ConnectionOptions
  {;; node info
   :addresses      [withAddresses   :< (varargs String)]
   :host           [withHost        :< str]
   :hosts          [withHosts       :< (varargs String)]
   :port           [withPort        :< int]
   :username       [withUsername    :< str]
   :password       [withPassword    :< str]
   :vhost          [withVirtualHost :< str]

   ;; SSL
   :ssl?           [withSsl]
   :ssl-context    [withSslProtocol :< identity]
   :ssl-protocol   [withSslProtocol :< str]

   ;; client info
   :name           [withName             :< str]
   :properties     [withClientProperties :< identity]

   ;; behaviour
   :socket-factory [withSocketFactory      :< identity]
   :factory        [withConnectionFactory  :< identity]
   :timeout        [withConnectionTimeout  :< (with to-duration {:none 0})]
   :executor       [withConsumerExecutor   :< identity]
   :heartbeat      [withRequestedHeartbeat :< (with to-duration {:none 0})]})
