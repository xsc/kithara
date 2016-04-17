(ns kithara.test.stack
  (:require [clojure.test.check.generators :as gen]))

(defn stack-gen
  "Based on generators for each layer of the stack, producing maps of
   `:forms` and `:build-fn`, generate a compound map describing the full stack."
  [& stack-gens]
  (gen/fmap
    (fn [results]
      {:forms     (vec (mapcat :forms results))
       :verifiers (into {} (map :verifiers results))
       :build-fn  #(reduce
                     (fn [value {:keys [build-fn]}]
                       (if build-fn
                         (build-fn value %2)
                         value))
                     %1 results)})
    (apply gen/tuple stack-gens)))

(defmacro stack-elements
  [bindings & options]
  `(gen/elements
     [~@(for [form options
              :let [verifiers (-> form meta :verifiers)]]
          `{:forms     [(quote ~(cons (first form) (next (next form))))]
            :verifiers ~(into {} (for [verifier verifiers]
                                   `[(quote ~verifier) ~verifier]))
            :build-fn  (fn [~@bindings] ~form)})]))

(defmacro optional-stack-elements
  [bindings & options]
  `(gen/one-of
     [(gen/return nil)
      (stack-elements ~bindings ~@options)]))
