(ns main
  (:require [cheshire.core :as json]
            [clj-boolean.analysis :refer [query-stats]]
            [clj-boolean.opt.core :refer [opt]]
            [clj-es-utils.query.analysis :refer [static-query-stats]]
            [clojure.pprint :refer [pprint]]
            [esopt.codegen :refer [emit-query]]
            [esopt.reverse :refer [es->ir]]))

(defn dump
  [data]
  (pprint data *err*))

(defn diff
  [o n]
  (let [change (- n o)
        pc (when (pos? o) (double (* (/ change o) 100)))]
    (cond
      (zero? change) "="
      (nil? pc) (format "%+d" change)
      :else (format "%+d (%.0f%%)" change (or pc 0.0)))))

(defn compare-stats
  [old-stats new-stats]
  (letfn [(merger [o n]
            (if (map? o)
              (compare-stats o n)
              (diff o n)))]
    (merge-with merger old-stats new-stats)))

(defn dump-query-stats
  [header stat-fn old new & {:keys [full-stats]}]
  (let [decorator (str (apply str (repeat (count header) \=)) \newline)]
    (.write *err* decorator)
    (.write *err* (str header \newline))
    (.write *err* decorator))
  (let [old-stats (stat-fn old)
        new-stats (stat-fn new)]
    (when full-stats
      (dump old-stats)
      (dump new-stats))
    (dump (compare-stats old-stats new-stats))))

(defn -main
  [& flags]
  (let [flag-set? (set (map keyword flags))
        input-query (json/parse-stream *in* true)
        ir (es->ir input-query)
        opt-ir (opt ir)
        output-query (emit-query opt-ir)]
    (when (flag-set? :ir-stats) (dump-query-stats "IR Stats" query-stats ir opt-ir :full-stats (flag-set? :full-stats)))
    (when (flag-set? :es-stats) (dump-query-stats "Elasticsearch Stats" static-query-stats input-query output-query :full-stats (flag-set? :full-stats)))
    (-> output-query
        (json/generate-string {:pretty (not (flag-set? :compact))})
        println))
  (shutdown-agents))
