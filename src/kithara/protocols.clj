(ns kithara.protocols
  "Basic protocols outlining kithara Components and their Composability."
  (:require [potemkin :refer [defprotocol+]]))

;; ## Wrappers

(defprotocol+ Wrapper
  "Protocol for component wrappers."
  (wrap-components [_ pred wrap-fn]
    "Apply the given function to each value contained within the wrapper
     matching the given predicate."))

(extend-protocol Wrapper
  clojure.lang.Sequential
  (wrap-components [this pred wrap-fn]
    (mapv
      (fn [value]
        (if (pred value)
          (wrap-fn value)
          (wrap-components value pred wrap-fn)))
      this))

  Object
  (wrap-components [this pred wrap-fn]
    (if (pred this)
      (wrap-fn this)
      this)))

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
       (defn ~(with-meta predicate {:no-doc true})
         ~(str "Check wheter the given value implements [[" id "]].")
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

(defn ^:no-doc consumer?
  "Check whether the given value implements [[Consumer]]."
  [value]
  (satisfies? Consumer value))

(defn ^:no-doc wrap-middleware
  [value wrap-fn]
  (wrap-components value consumer? #(add-middleware % wrap-fn)))

;; ## Coercer

;; ### Aliases

(defmulti coerce-by-keyword
  "Coerce the given byte array using the given, predefined coercer."
  {:arglists '([alias body])}
  (fn [k _]
    k)
  :default nil)

(defmethod coerce-by-keyword nil
  [k _]
  (throw
    (IllegalArgumentException.
      (str "unknown coercion alias:" k))))

(defmethod coerce-by-keyword :bytes
  [_ body]
  body)

(defmethod coerce-by-keyword :string
  [_ ^bytes body]
  (String. body "UTF-8"))

;; ### Protocol

(defprotocol+ Coercer
  "Protocol for byte array coercion. This is already implemented for:

   - the keyword `:bytes` (returns the original byte array)
   - the keyword `:string` (returns the byte array as a UTF-8 string)
   - `clojure.lang.AFn` (applies the function to the byte array).

   You can implement the multimethod [[coerce-by-keyword]] to add more aliases."
  (coerce [coercer ^bytes body]
    "Coerce the given byte array using the given coercer."))

(extend-protocol Coercer
  clojure.lang.Keyword
  (coerce [k body]
    (coerce-by-keyword k body))

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
  (with-meta '<hidden> {::secret data}))

(defn ^:no-doc reveal
  [data]
  {:pre [(= data '<hidden>)]}
  (-> data meta ::secret))

(defmacro ^:no-doc hide-constructors
  [record]
  (let [->var (fn [prefix] `(var ~(symbol (str prefix record))))]
    `(doseq [v# [~(->var "->") ~(->var "map->")]]
       (alter-meta! v# assoc :private true))))
