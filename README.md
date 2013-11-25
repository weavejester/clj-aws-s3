# clj-aws-s3

A Clojure library for accessing Amazon S3, based on the official AWS
Java SDK.

Although there are a few S3 clients for Clojure around, this library
aims to provide a more complete implementation, with metadata, streams
and protocols for uploading different types of data.

Currently the library supports functions to create, list and delete
buckets, to list, get, and put objects and their metadata, and to get
and update the access control lists (ACLs) for buckets and objects.

## Install

Add the following dependency to your `project.clj` file:

    [clj-aws-s3 "0.3.6"]

## Example

```clojure
(require '[aws.sdk.s3 :as s3])

(def cred {:access-key "...", :secret-key "..."})

(s3/create-bucket cred "my-bucket")

(s3/put-object cred "my-bucket" "some-key" "some-value")

(s3/update-object-acl cred "my-bucket" "some-key" (s3/grant :all-users :read))

(println (slurp (:content (s3/get-object cred "my-bucket" "some-key"))))
```

## Documentation

* [API docs](http://weavejester.github.com/clj-aws-s3/)

## License

Copyright © 2013 James Reeves

Distributed under the Eclipse Public License, the same as Clojure.
