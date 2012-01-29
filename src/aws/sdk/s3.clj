(ns aws.sdk.s3
  "Functions to access the Amazon S3 storage service.

  Each function takes a map of credentials as its first argument. The
  credentials map should contain an :access-key key and a :secret-key key."
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.s3.AmazonS3Client
           com.amazonaws.AmazonServiceException
           com.amazonaws.services.s3.model.ObjectMetadata
           com.amazonaws.services.s3.model.PutObjectRequest
           com.amazonaws.services.s3.model.S3Object
           java.io.ByteArrayInputStream
           java.io.File
           java.io.InputStream
           java.nio.charset.Charset))

(defn- s3-client
  "Create an AmazonS3Client instance from a map of credentials."
  [cred]
  (AmazonS3Client.
   (BasicAWSCredentials.
    (:access-key cred)
    (:secret-key cred))))

(defn bucket-exists?
  "Returns true if the supplied bucket name already exists in S3."
  [cred name]
  (.doesBucketExist (s3-client cred) name))

(defn create-bucket
  "Create a new S3 bucket with the supplied name."
  [cred name]
  (.createBucket (s3-client cred) name))

(defn delete-bucket
  "Delete the S3 bucket with the supplied name."
  [cred name]
  (.deleteBucket (s3-client cred) name))

(defprotocol ToPutRequest
  "A protocol for constructing a map that represents an S3 put request."
  (put-request [x] "Convert a value into a put request."))

(extend-protocol ToPutRequest
  InputStream
  (put-request [is] {:input-stream is})
  File
  (put-request [f] {:file f})
  String
  (put-request [s]
    {:input-stream     (ByteArrayInputStream. (.getBytes s))
     :content-length   (count s)
     :content-encoding (.name (Charset/defaultCharset))}))

(defmacro set-attr
  "Set an attribute on an object if not nil."
  {:private true}
  [object setter value]
  `(if-let [v# ~value]
     (~setter ~object v#)))

(defn- map->ObjectMetadata
  "Convert a map of object metadata into a ObjectMetadata instance."
  [metadata]
  (doto (ObjectMetadata.)
    (set-attr .setCacheControl         (:cache-control metadata))
    (set-attr .setContentDisposition   (:content-disposition metadata))
    (set-attr .setContentEncoding      (:content-encoding metadata))
    (set-attr .setContentLength        (:content-length metadata))
    (set-attr .setContentMD5           (:content-md5 metadata))
    (set-attr .setContentType          (:content-type metadata))
    (set-attr .setServerSideEncryption (:server-side-encryption metadata))
    (set-attr .setUserMetadata
     (dissoc metadata :cache-control
                      :content-disposition
                      :content-encoding
                      :content-length
                      :content-md5
                      :content-type
                      :server-size-encryption))))

(defn- ->PutObjectRequest
  "Create a PutObjectRequest instance from a bucket name, key and put request
  map."
  [bucket key request]
  (cond
   (:file request)
     (PutObjectRequest. bucket key (:file request))
   (:input-stream request)
     (PutObjectRequest.
      bucket key
      (:input-stream request)
      (map->ObjectMetadata (dissoc request :input-stream)))))

(defn put-object
  "Put a value into an S3 bucket at the specified key. The value can be
  a String, InputStream or File (or anything that implements the ToPutRequest
  protocol)."
  [cred bucket key value]
  (->> (put-request value)
       (->PutObjectRequest bucket key)
       (.putObject (s3-client cred))))

(defprotocol Mappable
  "Convert a value into a Clojure map."
  (to-map [x] "Return a map of the value."))

(extend-protocol Mappable
  S3Object
  (to-map [object]
    {:content  (.getObjectContent object)
     :metadata (to-map (.getObjectMetadata object))
     :bucket   (.getBucketName object)
     :key      (.getKey object)})
  ObjectMetadata
  (to-map [metadata]
    {:cache-control          (.getCacheControl metadata)
     :content-disposition    (.getContentDisposition metadata)
     :content-encoding       (.getContentEncoding metadata)
     :content-length         (.getContentLength metadata)
     :content-md5            (.getContentMD5 metadata)
     :content-type           (.getContentType metadata)
     :etag                   (.getETag metadata)
     :last-modified          (.getLastModified metadata)
     :server-side-encryption (.getServerSideEncryption metadata)}))

(defn get-object
  "Get an object from an S3 bucket. The object is returned as a map with the
  following keys:
    :content  - an InputStream to the content
    :metadata - a map of the object's metadata
    :bucket   - the name of the bucket
    :key      - the object's key"
  [cred bucket key]
  (to-map (.getObject (s3-client cred) bucket key)))

(defn get-object-metadata
  "Get an object's metadata from a bucket. The metadata is a map with the
  following keys:
    :cache-control          - the CacheControl HTTP header
    :content-disposition    - the ContentDisposition HTTP header
    :content-encoding       - the character encoding of the content
    :content-length         - the length of the content in bytes
    :content-md5            - the MD5 hash of the content
    :content-type           - the mime-type of the content
    :etag                   - the HTTP ETag header
    :last-modified          - the last modified date
    :server-side-encryption - the server-side encryption algorithm"
  [cred bucket key]
  (to-map (.getObjectMetadata (s3-client cred) bucket key)))
