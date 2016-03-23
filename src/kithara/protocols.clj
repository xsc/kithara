(ns kithara.protocols
  "Basic protocols outlining kithara Components and their Composability."
  (:require [potemkin :refer [defprotocol+]]))

;; ## Protccols

(defprotocol+ HasChannel
  (set-channel [_ channel]))

(defprotocol+ HasQueue
  (set-queue [_ queue]))

(defprotocol+ HasConnection
  (set-connection [_ connection]))

(defprotocol+ HasHandler
  (wrap-handler [_ wrap-fn]))

;; ## Predicates

(defmacro ^:private predicates
  [& protocols]
  `(do
     ~@(for [[protocol f] protocols]
         `(defn ~f
            ~(str "Check whether the given `value` implements [[" protocol "]].")
            [~'value]
            (satisfies? ~protocol ~'value)))))

(predicates
  [HasChannel    has-channel?]
  [HasQueue      has-queue?]
  [HasHandler    has-handler?]
  [HasConnection has-connection?])

;; ## Implement for Seqs

(defmacro ^:private extend-seq
  [& protocols]
  `(do
     ~@(for [[protocol f] protocols]
         `(extend-protocol ~protocol
            clojure.lang.Sequential
            (~f [this# val#] (map #(~f % val#) this#))))))

(extend-seq
  [HasChannel    set-channel]
  [HasQueue      set-queue]
  [HasHandler    wrap-handler]
  [HasConnection set-connection])

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
