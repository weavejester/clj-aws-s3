(ns aws.sdk.s3.client
  (:use [clojure.core cache memoize])
  (:import com.amazonaws.auth.AWSCredentials
           com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.auth.BasicSessionCredentials
           com.amazonaws.services.s3.AmazonS3Client
           clojure.core.memoize.PluggableMemoization))

;; Defines a cache implementation which maps arguments to keys using the
;; provided `key-for` function. The cached value for a given key is updated
;; anytime the arguments change.
(defcache KeyedCache [cache last-args-for key-for]
  CacheProtocol
  (lookup [_ args]
    (get cache (key-for args)))
  (lookup [_ args not-found]
    (get cache (key-for args) not-found))
  (has? [_ args]
    (let [key (key-for args)]
      (when-let [e (find last-args-for key)]
        (and (= (val e) args) ; detect change in args
             (contains? cache key)))))
  (hit [this args] this)
  (miss [_ args client]
    (let [key (key-for args)]
      (KeyedCache. (assoc cache key client)
                    (assoc last-args-for key args)
                    key-for)))
  (evict [_ args]
    (let [key (key-for args)]
      (KeyedCache. (dissoc cache key) last-args-for key-for)))
  (seed [_ base]
    (KeyedCache. base (keys base) key-for)))

(defn- keyed-client-cache []
  (KeyedCache. {} {} (fn [[cred]] (get cred :client-id cred))))

(defn- ^AWSCredentials aws-creds
  "Create an AWSCredentials implementation from a map of credentials."
  [{:keys [access-key secret-key session-token]}]
  (if session-token
    (BasicSessionCredentials. access-key secret-key session-token)
    (BasicAWSCredentials. access-key secret-key)))

(defn- s3-client*
  "Create an AmazonS3Client instance from a map of credentials."
  [cred]
  (let [client (AmazonS3Client. (aws-creds cred))]
    (when-let [endpoint (:endpoint cred)]
      (.setEndpoint client endpoint))
    client))

(def ^AmazonS3Client s3-client
  (build-memoizer
   #(PluggableMemoization. % (keyed-client-cache))
   s3-client*))
