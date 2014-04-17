(ns clj-whisper.wsp
  (:use clojure.java.io
        gloss.core
        gloss.io
        [clojure.tools.namespace.repl :only [refresh]]))

; http://graphite.readthedocs.org/en/1.0/whisper.html#database-format

(defcodec wsp-metadata
  (ordered-map :aggregation-method :uint32
               :max-retention :uint32
               :x-files-factor :float32
               :archive-count :uint32))

(defcodec archive-info
  (ordered-map :offset :uint32
               :seconds-per-point :uint32
               :points :uint32))

(defcodec wsp-point
  (ordered-map :timestamp :uint32
               :value :float64))

(defcodec wsp-data (repeated wsp-point :prefix :none))

(def aggregation-methods {:0 "unknown"
                          :1 "average"
                          :2 "sum"
                          :3 "last"
                          :4 "min"
                          :5 "max"
                          })


(defn print-archive-info [all-archives]
  (doall (map #(println (str "Archive " (.indexOf all-archives %) "\n"
                             "retention: " (* (:seconds-per-point %) (:points %)) "\n"
                             "secondsPerPoint: " (:seconds-per-point %) "\n"
                             "points: " (:points %) "\n"
                             "offset: " (:offset %)) "\n") all-archives)))

(defn print-whisper-info [filename]
  (let [data (with-open [f (input-stream filename)]
               (let [size (.length (file filename))
                     ba (byte-array size)]
                 (.read f ba)
                 (to-byte-buffer ba)))
        filesize (.length (file filename))
        wmetadata (decode wsp-metadata data false)]
    (do
      (defcodec archives
        (ordered-map :metadata wsp-metadata
                     :archive-info (repeat (:archive-count wmetadata) archive-info)))
      (println (str "maxRetention: " (:max-retention wmetadata)))
      (println (str "xFilesFactor: " (:x-files-factor wmetadata)))
      (println (str "aggregationMethod: " ((keyword (str (:aggregation-method wmetadata))) aggregation-methods)))
      (println (str "fileSize: " filesize))
      (println)
      (print-archive-info (:archive-info (decode archives data false)))
      (println))))

(defn whisper-read [filename]
  (let [data (with-open [f (input-stream filename)]
               (let [size (.length (file filename))
                     ba (byte-array size)]
                 (.read f ba)
                 (to-byte-buffer ba)))
        filesize (.length (file filename))
        wmetadata (decode wsp-metadata data false)]
    (do
      (defcodec archives
        (ordered-map :metadata wsp-metadata
                     :archive-info (repeat (:archive-count wmetadata) archive-info)
                     :points wsp-data))
      (let [wsp-all (decode archives data)]
        {:metadata (:metadata wsp-all)
         :archive-info (:archive-info wsp-all)
         :points (apply merge (map #(sorted-map
                                       (keyword (str (:timestamp %)))
                                       (:value %)) (:points wsp-all)))}
          ))))

; returns size of archive in bytes
(defn archive-size [archive-info]
  (* (:points archive-info) 12)) 

; retention time of archive in seconds
(defn archive-retention [archive-info]
  (* (:seconds-per-point archive-info) (:points archive-info)))

; returns byte offset of the last point in an archive
(defn archive-end [archive-info]
  (+ (:offset archive-info) (archive-size archive-info)))

(declare now)
(declare yesterday)
(defn whisper-fetch 
  "Returns map of points within the specified time range"
  [wsp & {:keys [from until]
          :or {from yesterday until now}}]
    (let [points (:points wsp)
          now (int (/ (System/currentTimeMillis) 1000))
          yesterday (- now (* 60 60 24))]
    (apply sorted-map 
           (flatten 
             (subseq 
               (apply sorted-map
                      (flatten 
                        (subseq points < (keyword (str until)))))
               > (keyword (str from)))))))



