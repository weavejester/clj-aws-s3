(ns aws.sdk.s3
  "Functions to access the Amazon S3 storage service.

  Each function takes a map of credentials as its first argument. The
  credentials map should contain an :access-key key and a :secret-key key."
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.s3.AmazonS3Client
           com.amazonaws.AmazonServiceException
           com.amazonaws.services.s3.model.ObjectMetadata))

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
