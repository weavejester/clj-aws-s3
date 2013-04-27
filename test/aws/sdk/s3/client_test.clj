(ns aws.sdk.s3.client-test
  (:use [aws.sdk.s3.client]
        [clojure.test])
  (:import com.amazonaws.services.s3.AmazonS3Client
           com.amazonaws.auth.BasicAWSCredentials))

(deftest amazon-s3-client-test
  (testing "AmazonS3Client Constructor"
    (let [cred (BasicAWSCredentials. "XYZ" "123")]
      (is (not= (AmazonS3Client. cred) (AmazonS3Client. cred))
          "returns different client for different args"))))

(deftest s3-client-test
  (testing "memoization without :client-id"
    (let [cred {:access-key "XYZ" :secret-key "123"}]
      (is (= (s3-client cred) (s3-client cred))
          "returns the same object when given the same arguments"))

    (let [cred1 {:access-key "XYZ" :secret-key "123"}
          cred2 {:access-key "ABC" :secret-key "789"}]
      (is (not (= (s3-client cred1) (s3-client cred2)))
          "returns the different objects for different arguments")))

  (testing "memoization with :client-id"
    (let [cred {:access-key "XYZ" :secret-key "123" :client-id (Object.)}]
      (is (= (s3-client cred) (s3-client cred))
          "returns the same object when given the same arguments"))

    (let [cred1 {:access-key "XYZ" :secret-key "123" :client-id (Object.)}
          cred2 {:access-key "XYZ" :secret-key "123" :client-id (Object.)}]
      (is (not (= (s3-client cred1) (s3-client cred2)))
          "returns the different objects for different :client-id's"))

    (let [id (Object.)
          cred1 {:access-key "XYZ" :secret-key "123" :client-id id}
          cred2 {:access-key "ABC" :secret-key "789" :client-id id}
          client1 (s3-client cred1)
          client2 (s3-client cred2)
          client3 (s3-client cred1)]
      (is (not (= client1 client2))
          "returns the different objects for differnt arguments and the same :client-id")
      (is (not (= client1 client3))
          "caches only one client per :client-id"))))
