(ns clj-whisper.devel
  (:use clj-whisper.wsp
        [gloss.io :only [to-byte-buffer decode]]
        [clojure.java.io :only [file input-stream]]
        [clojure.tools.namespace.repl :only [refresh]]))

(def filename "proc_run.wsp")

(def data (with-open [f (input-stream filename)]
            (let [size (.length (file filename))
                  ba (byte-array size)]
              (.read f ba)
              (to-byte-buffer ba))))

