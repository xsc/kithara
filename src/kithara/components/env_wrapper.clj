(ns kithara.components.env-wrapper
  (:require [kithara.protocols :as p]
            [kithara.middlewares.env :refer [wrap-env]]
            [peripheral.core :as peripheral :refer [defcomponent]]))

;; ## Internal Component

(defn- prepare-components
  [{:keys [components env]}]
  (p/wrap-middleware components #(wrap-env % env)))

(defcomponent InternalEnvWrapper [components env]
  :this/as            *this*
  :assert/components? (seq components)
  :components/running (prepare-components *this*))

(p/hide-constructors InternalEnvWrapper)

;; ## Component

(defn- start-internal-env-wrapper!
  [this]
  (let [env (dissoc this ::internal)]
    (update this ::internal #(-> % (update :env merge env) peripheral/start))))

(defn- stop-internal-env-wrapper!
  [this]
  (update this ::internal peripheral/stop))

(defcomponent EnvWrapper []
  :peripheral/start start-internal-env-wrapper!
  :peripheral/stop  stop-internal-env-wrapper!)

(p/hide-constructors EnvWrapper)

;; ## Wrapper

(defn with-env
  "Let each message have an `:env` key with the message handling environment.
   It is possible to `assoc` more keys into the resulting component before
   startup, allowing it to participate in `com.stuartsierra/component` systems,
   e.g.:

   ```
   (defonce system
     (component/system-map
       :consumer (-> message-handler
                     (kithara.core/consumer ...)
                     ...
                     (kithara.core/with-env)
                     (component/using [:db :elastic]))
       :db       (map->DB {...})
       :elastic  (map->ES {...})))
   ```

   For this to work, `with-env` has to be top-most layer of the kithara
   consumer. If you're using the two-parameter version, you can explicitly
   specify the environment to inject."
  ([components]
   (with-env components {}))
  ([components env]
   (let [internal (map->InternalEnvWrapper
                    {:components (p/consumer-seq components)
                     :env        {}})]
     (into (map->EnvWrapper {::internal internal}) env))))
