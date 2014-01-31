(defproject clj-aws-s3 "0.3.8"
  :description "Clojure Amazon S3 library."
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [com.amazonaws/aws-java-sdk "1.4.2.1"]
                 [clj-time "0.5.0"]]
  :plugins [[codox "0.6.4"]
            [com.backstopsolutions/bsg-lein-publish "0.0.6"]]
  :repositories [["backstop" "http://ivy.backstopsolutions"]])
