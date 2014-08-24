;; to run the tests you need an aws account
;; and a file test/aws/sdk/aws.clj that contains something like
;; (def key "...")
;; (def skey "...")
;; (def test-bucket "...")
;; for your aws account

(ns aws.sdk.s3-test
  (:refer-clojure :exclude [key])
  (:load "aws") ;; aws credentials and test bucket name
  (:use [aws.sdk.s3]
        [clojure.test])
  (:require [clojure.java.io :as io])
  (:import (java.util UUID)
           (java.io ByteArrayInputStream
                    ByteArrayOutputStream)))

(def ^{:dynamic true} *creds*)

(use-fixtures :once
  (fn [f]
    (create-bucket {:access-key key
                    :secret-key skey}
                   test-bucket)
    (binding [*creds* {:access-key key
                       :secret-key skey}]
      (f))))

(deftest t-test-bucket-exists
  (is (contains? (set (map :name (list-buckets *creds*))) test-bucket)))

(deftest t-put-object
  (let [t-key   "test-key"
        t-value "test-value"
        t-req   (put-object *creds* test-bucket t-key t-value)]
    (is (contains? (set (map :key (:objects (list-objects *creds* test-bucket)))) t-key))))

(deftest t-put-multipart-stream
  (let [t-key    "test-key"
        t-value  (apply str (take 3000 (repeat "testword ")))
        t-stream (io/input-stream (.getBytes t-value))
        t-req    (put-multipart-object *creds* test-bucket t-key t-stream)]
    (is (object-exists? *creds* test-bucket t-key))
    (is (= (slurp (:content (get-object *creds* test-bucket t-key)))
           t-value))
    (delete-object *creds* test-bucket t-key)))

(comment
  (def c
    {:access-key key
     :secret-key skey})

  (let [t-key    "test-key"
        t-value  (apply str (take 3000 (repeat "testword ")))
        t-stream (io/input-stream (.getBytes t-value))
        t-req    (put-multipart-stream c test-bucket t-key t-stream)]
    t-req)

  (list-objects c test-bucket))
