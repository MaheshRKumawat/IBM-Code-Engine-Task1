package main

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
)

func Cos_Connect(apiKey, serviceInstanceID, authEndpoint, serviceEndpoint, bucketName string) (ob_keys []string, bucket *s3.ListObjectsV2Output) {

	// Create config
	conf := aws.NewConfig().
		WithRegion("us-standard").
		WithEndpoint(serviceEndpoint).
		WithCredentials(ibmiam.NewStaticCredentials(aws.NewConfig(), authEndpoint, apiKey, serviceInstanceID)).
		WithS3ForcePathStyle(true)

	sess := session.Must(session.NewSession())
	client := s3.New(sess, conf)

	list_objects := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	fmt.Println("List Objects ", list_objects)
	bucket, _ = client.ListObjectsV2(list_objects)
	fmt.Println("\nBucket: ", bucket)
	fmt.Printf("Bucket type: %s", reflect.TypeOf(bucket))

	type ob []map[string]string
	var jsonMap map[string]ob

	jsonBytes, _ := json.MarshalIndent(list_objects, " ", " ")
	json.Unmarshal(jsonBytes, &jsonMap)
	objects := jsonMap["Contents"]

	for _, v := range objects {
		ob_keys = append(ob_keys, v["Key"])
	}

	return ob_keys, bucket
}
