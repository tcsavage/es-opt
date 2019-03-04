(ns esopt.codegen
  (:require [clj-boolean.syntax :as syn]
            [clj-boolean.timing :as timing]
            [clj-es-utils.query.core :as es]
            [clojure.tools.logging :as log]))

(defmulti emit-node syn/node-tag)

(defn emit-query
  [query]
  (log/info "Starting Elasticsearch query code generation.")
  (let [[es-query codegen-time] (timing/time-ret (syn/walk-query emit-node query))]
    (log/infof "Finished generating Elasticsearch query code. Time taken: %.2f msecs." codegen-time)
    es-query))

(defmethod emit-node :clj-boolean.syntax/and
  [{terms :children}]
  {:bool {:filter terms}})

(defn bool-branches
  [node]
  "Returns set of branch types #{:must :must_not :should :filter} in a bool query node."
  (let [branches #{:must :must_not :filter :should}]
    (set (filter #(seq (get-in node [:bool %])) branches))))

(defn term-target
  [node]
  "Returns target field of a term/terms query node."
  (some->> node vals first keys first))

(defn terms-values
  "Returns all values in terms query node."
  [node]
  (-> node :terms vals first))

(defn term->terms
  "Turns a term query node into a terms with a single value."
  [node]
  (let [target (term-target node)]
    {:terms {target [(get-in node [:term target])]}}))

(defn simplify-terms
  "If the terms query node only has a single value, turns it into a term node."
  [node]
  (let [target (term-target node)
        values (get-in node [:terms target])]
    (if (> (count values) 1)
      node
      {:term {target (first values)}})))

(defn collapse-terms
  "Returns a single collection of term/terms query nodes."
  [term-nodes terms-nodes]
  (let [nodes (concat terms-nodes (map term->terms term-nodes))
        groups (group-by term-target nodes)]
    (for [[target group] groups]
      (simplify-terms
       {:terms {target (distinct (mapcat terms-values group))}}))))

(defn dissoc-in
  [x path]
  (update-in x (butlast path) dissoc (last path)))

(defn clean-bool
  [node]
  "Simplifies a bool query node which only has a should or must branch with a single child."
  (let [branches (bool-branches node)]
    (cond
      (= #{:should} branches)
      (let [children (get-in node [:bool :should])]
        (if (= 1 (count children))
          (first children)
          {:bool {:should children :minimum_should_match 1}}))
      (= #{:must} branches)
      (let [children (get-in node [:bool :must])]
        (if (= 1 (count children))
          (first children)
          {:bool {:must children}}))
      :else (cond-> node
              (-> node :bool :must empty?)     (dissoc-in [:bool :must])
              (-> node :bool :must_not empty?) (dissoc-in [:bool :must_not])
              (-> node :bool :filter empty?)   (dissoc-in [:bool :filter])
              (-> node :bool :should empty?)   (dissoc-in [:bool :should])))))

(defn es-or-base
  [terms]
  {:bool {:should terms :minimum_should_match 1}})

(defn es-or-collapse-ids
  "Transforms a bool/should assembler such that it collapses multiple ids queries together if possible."
  [es-or-fn]
  (fn [terms]
    (log/debugf "Collapsing ids inside an OR: %s" (pr-str (map es/node-tag terms)))
    (let [ids-nodes (filter #(= :ids (es/node-tag %)) terms)
          remaining-nodes (remove #(= :ids (es/node-tag %)) terms)
          ids (map (comp :values :ids) ids-nodes)
          unique-ids (vec (distinct ids))
          collapsed-ids (when (seq unique-ids) {:ids {:values unique-ids}})]
      (log/debugf "Collapsed %d ids nodes into 1 (%d ids to %d)" (count ids-nodes) (count ids) (count unique-ids))
      (es-or-fn (if collapsed-ids (cons collapsed-ids remaining-nodes) remaining-nodes)))))

(defn es-or-collapse-terms
  "Transforms a bool/should assembler such that it collapses multiple term/terms queries together if possible."
  [es-or-fn]
  (fn [terms]
    (log/debugf "Collapsing terms inside an OR: %s" (pr-str (map es/node-tag terms)))
    (let [term-nodes (filter #(= :term (es/node-tag %)) terms)
          terms-nodes (filter #(= :terms (es/node-tag %)) terms)
          remaining-nodes (remove #(#{:term :terms} (es/node-tag %)) terms)
          collapsed-terms (collapse-terms term-nodes terms-nodes)]
      (es-or-fn (concat remaining-nodes collapsed-terms)))))

(defn part
  [f xs]
  ((juxt (partial filter f) (partial remove f)) xs))

(defn es-or-collapse-bools
  "Transforms a bool/should assembler such that it collapses child bool query nodes into itself."
  [es-or-fn]
  (fn [terms]
    (let [bool-terms (filter #(= :bool (es/node-tag %)) terms)
          remaining-terms (remove #(= :bool (es/node-tag %)) terms)
          [shoulds others] (part #(= #{:should} (bool-branches %)) bool-terms)
          should-terms (map (comp :should :bool) shoulds)]
      (es-or-fn (apply concat remaining-terms others should-terms)))))

(def es-or
  (-> es-or-base
      es-or-collapse-ids
      es-or-collapse-terms
      es-or-collapse-bools))

(defmethod emit-node :clj-boolean.syntax/or
  [{terms :children}]
  (es-or terms))

(defmethod emit-node :clj-boolean.syntax/not
  [{term :child}]
  {:bool {:must_not [term]}})

;; ES generators

(defmethod emit-node :es/term
  [{field :field term :term}]
  {:term {field term}})

(defmethod emit-node :es/match
  [{fields :fields match :match}]
  (if (> (count fields) 1)
    {:multi_match {:query match
                   :fields fields
                   :type "phrase"}}
    {:match {(first fields) match}}))

(defmethod emit-node :es/nested
  [{path :path child :child}]
  {:nested {:path path
            :query child}})

(defmethod emit-node :es/range
  [{field :field opts :opts}]
  {:range {:field opts}})

(defmethod emit-node :es/id
  [{id :id}]
  {:ids {:values [id]}})

