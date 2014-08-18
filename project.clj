(defproject clj-aws-s3 "0.3.9-w"
  :description "Clojure Amazon S3 library"
  :url "https://github.com/weavejester/clj-aws-s3"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [com.amazonaws/aws-java-sdk "1.7.5"]
                 [clj-time "0.6.0"]]
  :plugins [[codox "0.6.7"]
            [s3-wagon-private "1.1.2"]]
  :deploy-repositories [["releases" {:url "s3p://eng-repos/libs/releases/" :creds :gpg}]])

