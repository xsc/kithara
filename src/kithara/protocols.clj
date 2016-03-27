(ns kithara.protocols
  "Basic protocols outlining kithara Components and their Composability."
  (:require [potemkin :refer [defprotocol+]]))

;; ## Wrappers

(defprotocol+ Wrapper
  "Protocol for wrappers."
  (wrap-components [_ pred wrap-fn]
    "Apply the given function to each value contained within the wrapper
     matching the given predicate.")
  (unwrap [_]
    "Retrieve a seq of all values within the wrapper."))

(extend-protocol Wrapper
  clojure.lang.Sequential
  (wrap-components [this pred wrap-fn]
    (mapv
      (fn [value]
        (if (pred value)
          (wrap-fn value)
          (wrap-components value pred wrap-fn)))
      this))
  (unwrap [this]
    (mapcat unwrap this))

  Object
  (wrap-components [this pred wrap-fn]
    (if (pred this)
      (wrap-fn this)
      this))
  (unwrap [this]
    []))

;; ## Injections

;; ### Helper Macro

(defmacro ^:private definjection
  [id [injects] doc]
  (let [predicate (symbol (str "has-" injects "?"))
        wrapper (symbol (str "wrap-" injects))
        setter (symbol (str "set-" injects))]
    `(do
       (defprotocol+ ~id
         ~doc
         (~setter [~'value ~injects]
           ""))
       (defn ~predicate
         ~(str "Check wheter the given value implements `" id "`.")
         [~'value]
         (satisfies? ~id ~'value))
       (defn ~(with-meta wrapper {:no-doc true})
         ~(str "Use `wrap-components` to apply `" setter
               "` to all values implementing `" id "`.")
         [~'value ~injects]
         (wrap-components ~'value ~predicate #(~setter % ~injects))))))

;; ### Implementations

(definjection HasChannel [channel]
  "Protocol for components depending on a channel.")

(definjection HasConnection [connection]
  "Protocol for components depending on a connection.")

(definjection HasQueue [queue]
  "Protocol for components depending on a queue.")

;; ## Consumer

(defprotocol+ Consumer
  "Protocol for consumers."
  (add-middleware [_ wrap-fn]
    "Wrap the consumer handler function using the given `wrap-fn`."))

(defn consumer?
  [value]
  (satisfies? Consumer value))

(defn wrap-middleware
  [value wrap-fn]
  (wrap-components value consumer? #(add-middleware % wrap-fn)))

;; ## Coercer

(defprotocol+ Coercer
  (coerce [_ ^bytes body]))

(extend-protocol Coercer
  clojure.lang.Keyword
  (coerce [k body]
    (case k
      :bytes                 body
      (:string :utf8-string) (String. ^bytes body "UTF-8")
      (throw
        (IllegalArgumentException.
          (str "unknown coercion target: " k)))))

  clojure.lang.AFn
  (coerce [f body]
    (f body)))

;; ## Helper

(defn ^:no-doc consumer-seq
  [value]
  (if (sequential? value)
    (vec value)
    [value]))

(defn ^:no-doc hide
  [data]
  (with-meta '<hidden> ::secret data))

(defn ^:no-doc reveal
  [data]
  {:pre [(= data '<hidden>)]}
  (-> data meta ::scret))

(defmacro ^:no-doc hide-constructors
  [record]
  (let [->var (fn [prefix] `(var ~(symbol (str prefix record))))]
    `(doseq [v# [~(->var "->") ~(->var "map->")]]
       (alter-meta! v# assoc :private true))))
