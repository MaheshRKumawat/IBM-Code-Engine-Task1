package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"

	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
)

func reduce(fileName string) {
	fi, erri := os.Open(fileName)

	if erri != nil {
		fmt.Printf("Failed opening file, error: %s", erri)
		os.Exit(1)
	}

	totalSale := map[string]int64{}
	price := map[string]float64{}
	csvReader := csv.NewReader(fi)

	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error in reading records, error: ", err)
		} else {
			quant, errQ := strconv.ParseInt(rec[1], 10, 64)
			pr, errP := strconv.ParseFloat(rec[2], 10)

			if errQ != nil || errP != nil {
				// For non-int values
				continue
			}
			totalSale[rec[0]] += quant
			price[rec[0]] = pr
		}
	}
	fi.Close()

	frq, errq := os.Create("salesQuantity.csv")
	frp, errp := os.Create("salesPrice.csv")
	if errq != nil {
		log.Fatalf("Failed creating file, error: %s", errq)
	}
	if errp != nil {
		log.Fatalf("Failed creating file, error: %s", errp)
	}

	// Sorting Algo
	keys := []string{}
	for key := range totalSale {
		keys = append(keys, key)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return totalSale[keys[i]] > totalSale[keys[j]]
	})

	csvWriterQ := csv.NewWriter(frq)
	csvWriterP := csv.NewWriter(frp)

	for _, name := range keys {
		quantStr := strconv.Itoa(int(totalSale[name]))
		crQ := []string{name, quantStr}
		_ = csvWriterQ.Write(crQ)
	}

	for _, name := range keys {
		crP := []string{name, strconv.FormatFloat(price[name], 'g', 8, 64)}
		_ = csvWriterP.Write(crP)
	}

	// You need to call the Flush method of your CSV writer to ensure all buffered data is written to your file before closing the file.
	csvWriterQ.Flush()
	csvWriterP.Flush()
	frq.Close()
	frp.Close()
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
	key := os.Getenv("MAPPER_OBJECT_KEY")

	// users will need to create bucket, key (flat string name)
	Input := s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	// Call Function
	res, _ := client.GetObject(&Input)

	body, _ := ioutil.ReadAll(res.Body)

	reduce(string(body))

	// Variables and random content to sample, replace when appropriate
	Newkey := os.Getenv("REDUCER_OBJECT_KEY")
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
