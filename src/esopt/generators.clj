(ns esopt.generators
  (:require [clj-boolean.analysis :as pass-distrib]
            [clj-boolean.pretty :as pretty]
            [clj-boolean.syntax :as syn]
            [clojure.core :as cc])
  (:refer-clojure :exclude [range]))

(defn term
  [field term]
  (syn/node :es/term :field field :term term))

(defn match
  [fields match]
  (syn/node :es/match :fields fields :match match))

(defn range
  [field & {:as opts}]
  (syn/node :es/range :field field :opts opts))

(defn nested
  [path child]
  (syn/node :es/nested :path path :child child))

