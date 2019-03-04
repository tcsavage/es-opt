(ns main
  (:require [cheshire.core :as json]
            [clj-boolean.opt.core :refer [opt]]
            [clj-es-utils.query.analysis :refer [static-query-stats]]
            [clojure.pprint :refer [pprint]]
            [esopt.codegen :refer [emit-query]]
            [esopt.reverse :refer [es->ir]]))

(defn dump-query-stats
  [query]
  (pprint (static-query-stats query) *err*)
  query)

(defn -main
  []
  (-> *in*
      (json/parse-stream true)
      dump-query-stats
      es->ir
      opt
      emit-query
      dump-query-stats
      (json/generate-string {:pretty true})
      println)
  (shutdown-agents))
