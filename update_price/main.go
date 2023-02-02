package main

import (
	"bytes"
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

func updatePrice(priceFile string) {
	fp, errp := os.Open(priceFile)

	if errp != nil {
		log.Fatalf("Failed opening file, error: %s", errp)
	}

	csvReaderP := csv.NewReader(fp)
	price := map[string]float64{}
	keys := []string{}
	length := 0
	count := 0

	for {
		rec, err := csvReaderP.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error in reading records, error: ", err)
		} else {
			pr, errP := strconv.ParseFloat(rec[1], 10)

			if errP != nil {
				continue
			}
			price[rec[0]] = pr
			keys = append(keys, rec[0])
			length++
		}
	}
	fp.Close()

	for _, key := range keys {
		if count <= length/10 { // Decimal value will be rounded off to floor value
			price[key] += price[key] * 0.1
		} else if count >= (length - (length / 10)) {
			price[key] -= (price[key] * 0.1)
		}
		count++
	}

	fo, erro := os.Create("UpdatedPrice.csv")
	csvWriter := csv.NewWriter(fo)
	if erro != nil {
		log.Fatalf("Failed creating file, error: %s", erro)
	}

	for key, value := range price {
		prc := []string{key, strconv.FormatFloat(value, 'g', 8, 64)}
		_ = csvWriter.Write(prc)
	}
	csvWriter.Flush()
	fo.Close()
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

	d, err := client.ListBuckets(&s3.ListBucketsInput{})

	fmt.Print(d)

	fmt.Println("error: ", err)

	// Variables
	bucketName := os.Getenv("BUCKET_NAME")
	key := os.Getenv("REDUCER_OBJECT_KEY")

	// users will need to create bucket, key (flat string name)
	Input := s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	// Call Function
	res, _ := client.GetObject(&Input)

	body, _ := ioutil.ReadAll(res.Body)

	updatePrice(string(body))

	// Variables and random content to sample, replace when appropriate
	Newkey := os.Getenv("FINAL_OBJECT_KEY")
	content := bytes.NewReader([]byte("<CONTENT>"))

	input := s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(Newkey),
		Body:   content,
	}

	// Call Function to upload (Put) an object
	result, _ := client.PutObject(&input)
	fmt.Println(result)
}
