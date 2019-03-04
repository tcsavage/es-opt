(ns esopt.reverse
  "Try to generate IR from an Elasticsearch query."
  (:require [clj-boolean.syntax :as syn]
            [clj-es-utils.query.core :as es]
            [clojure.tools.logging :as log]
            [esopt.generators :as gen]))

(def ignored-fields #{:boost :adjust_pure_negative :minimum_should_match})
(def extract-value (some-fn :value :values :query))

(defn query-value [data] (if (map? data) (extract-value data) data))
(defn select-query [data] (->> data (remove (comp ignored-fields key)) first val))
(defn select-field [data] (->> data (remove (comp ignored-fields key)) first key))

(defmulti es->ir-node es/node-tag)

(defn es->ir
  [query]
  (es/walk-query es->ir-node query))

(defmethod es->ir-node :default
  [node]
  (throw (ex-info (format "Don't know how to translate ES node (%s) back to IR" (es/node-tag node)) {:node node})))

(defmethod es->ir-node :multi_match
  [{match :multi_match}]
  (gen/match (:fields match) (:query match)))

(defmethod es->ir-node :match
  [{match :match}]
  (let [[field query] (first match)]
    (gen/match [field] query)))

(defmethod es->ir-node :terms
  [{terms :terms}]
  (let [field (name (select-field terms))
        values (-> terms select-query query-value)]
    (apply syn/or (map (partial gen/term field) values))))

(defmethod es->ir-node :term
  [{term :term}]
  (let [field (name (select-field term))
        value (-> term select-query query-value)]
    (gen/term field value)))

(defmethod es->ir-node :nested
  [{nested :nested}]
  (let [path (:path nested)
        query (:query nested)]
    (gen/nested path query)))

(defmethod es->ir-node :range
  [{range :range}]
  (let [field (name (select-field range))
        query (select-query range)]
    (apply gen/range field (apply concat query))))

(defmethod es->ir-node :bool
  [{{filter :filter should :should must :must must-not :must_not} :bool}]
  (let [anded (when (or (seq filter) (seq must)) (concat must filter))
        ored (when (seq should) should)
        ored-not (when (seq must-not) must-not)]
    (syn/and
     (some->> anded (apply syn/and))
     (some->> ored (apply syn/or))
     (some->> ored-not (apply syn/or) syn/not))))
