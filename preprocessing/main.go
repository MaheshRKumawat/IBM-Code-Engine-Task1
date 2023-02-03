package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
)

func preprocess(fileName string) (rec string) {
	fi, erri := os.Open(fileName)
	fo, erro := os.Create("salesP.csv")

	if erri != nil {
		fmt.Printf("Failed opening file, error: %s", erri)
		os.Exit(1)
	}
	if erro != nil {
		fmt.Printf("Failed creating file, error: %s", erro)
		os.Exit(1)
	}

	csvReader := csv.NewReader(fi)
	csvWriter := csv.NewWriter(fo)

	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error in reading records, error: ", err)
		} else {
			// Check if there is a valid price and quantity values in the dataset.
			_, errQ := strconv.ParseInt(rec[2], 10, 64)
			_, errP := strconv.ParseFloat(rec[3], 10)

			if errQ != nil || errP != nil {
				// For non-int values
				continue
			} else {
				_ = csvWriter.Write(rec)
			}
		}
	}
	// You need to call the Flush method of your CSV writer to ensure all buffered data is written to your file before closing the file.
	csvWriter.Flush()

	fi.Close()
	fo.Close()

	return rec
}

func main() {
	apiKey := os.Getenv("API_KEY")
	serviceInstanceID := os.Getenv("RESOURCE_INSTANCE_ID")
	authEndpoint := os.Getenv("AUTH_ENDPOINT")
	serviceEndpoint := os.Getenv("SERVICE_ENDPOINT")
	// bucketLocation := os.Getenv("LOCATION")                                   

	// Create config
	conf := aws.NewConfig().
		WithRegion("us-standard").
		WithEndpoint(serviceEndpoint).
		WithCredentials(ibmiam.NewStaticCredentials(aws.NewConfig(), authEndpoint, apiKey, serviceInstanceID)).
		WithS3ForcePathStyle(true)

	// Create client
	sess := session.Must(session.NewSession())
	client := s3.New(sess, conf)

	_, err := client.ListBuckets(&s3.ListBucketsInput{})

	// fmt.Print(d)

	fmt.Println("error: ", err)

	// Variables
	bucketName := os.Getenv("BUCKET_NAME")
	key := os.Getenv("DATASET_OBJECT_KEY")

	// users will need to create bucket, key (flat string name)
	Input := s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	// Call Function
	res, _ := client.GetObject(&Input)

	body, _ := ioutil.ReadAll(res.Body)

	preprocess(string(body))

	// fmt.Print(body)

	// // Variables and random content to sample, replace when appropriate
	// Newkey := os.Getenv("PREPROCESSED_OBJECT_KEY")
	// content := bytes.NewReader([]byte(contentfile))

	// input := s3.PutObjectInput{
	// 	Bucket: aws.String(bucketName),
	// 	Key:    aws.String(Newkey),
	// 	Body:   content,
	// }

	// // Call Function to upload (Put) an object
	// result, _ := client.PutObject(&input)
	// fmt.Println(result)
}
