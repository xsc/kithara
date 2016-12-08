(ns kithara.config
  "Lyra configuration builders."
  (:import [net.jodah.lyra.config
            RecoveryPolicies
            RetryPolicies]
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
                (cond (= processor ::none) param
                      (sequential? processor) `(~@processor ~param)
                      :else `(~processor ~param))))))
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
  (let [max-len (+ 3 (apply max (map (comp count name key) mapping)))
        padded-fmt (str "%-" max-len "s")
        padded #(format padded-fmt %)
        sep (apply str \: (repeat (dec max-len) \-))]
    {:arglists '([] [configuration-map] [base configuration-map])
     :tag      result-class
     :doc      (str (apply str
                           "Generates a `" result-class "` (see Lyra documentation).\n\n"
                           "| " (padded "option") " | |\n"
                           "| " sep " | ------------ |\n"
                           (for [[k [f]] (sort-by key mapping)]
                             (format "| %s | delegates to `%s` |\n"
                                     (padded (str "`" k "`"))
                                     f)))
                    "\n`base` can be an instance of `" result-class "` in which case all "
                    "of the given options will be applied to it.")}))

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
  (^long [value] (int value))
  (^long [k default value]
   (to-int (get value k default))))

(defn- to-duration
  (^Duration
    [value]
    (if (instance? Duration value)
      value
      (Duration/milliseconds value)))
  (^Duration
    [k default value]
    (let [n (get value k default)]
      (to-duration n))))

(defmacro ^:private varargs
  [fully-qualified-class value]
  (with-meta
    `(into-array ~fully-qualified-class ~value)
    {:tag (str "[L" fully-qualified-class ";")}))

(defmacro ^:private with
  [f mapping value]
  `(~f (let [v# ~value] (get ~mapping v# v#))))

(defmacro ^:private arg
  [class value]
  (with-meta
    `(identity ~value)
    {:tag class}))

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

;; ## Behaviour Config

(defconfig behaviour :> net.jodah.lyra.config.Config
  {:recovery-policy      [withRecoveryPolicy      :< recovery-policy]
   :retry-policy         [withRetryPolicy         :< retry-policy]
   :queue-recovery?      [withQueueRecovery       :< boolean]
   :exchange-recovery?   [withExchangeRecovery    :< boolean]
   :consumer-recovery?   [withConsumerRecovery    :< boolean]
   :connection-listeners [withConnectionListeners :< (varargs net.jodah.lyra.event.ConnectionListener)]
   :channel-listeners    [withChannelListeners    :< (varargs net.jodah.lyra.event.ChannelListener)]
   :consumer-listeners   [withConsumerListeners   :< (varargs net.jodah.lyra.event.ConsumerListener)]})

;; ## Connection Options

(defconfig connection :> net.jodah.lyra.ConnectionOptions
  {;; node info
   :addresses      [withAddresses   :< (varargs java.lang.String)]
   :host           [withHost        :< str]
   :hosts          [withHosts       :< (varargs java.lang.String)]
   :port           [withPort        :< int]
   :username       [withUsername    :< str]
   :password       [withPassword    :< str]
   :vhost          [withVirtualHost :< str]

   ;; SSL
   :ssl?           [withSsl]
   :ssl-context    [withSslProtocol :< (arg javax.net.ssl.SSLContext)]
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
