package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
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

func preprocess(fileName string) {
	fi, erri := os.Open(fileName)
	fo, erro := os.Create("Preprocessed.csv")

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
}

func main() {
	apiKey := os.Getenv("API_KEY")
	serviceInstanceID := os.Getenv("RESOURCE_INSTANCE_ID")
	authEndpoint := os.Getenv("AUTH_ENDPOINT")
	serviceEndpoint := os.Getenv("SERVICE_ENDPOINT")
	bucketName := os.Getenv("BUCKET_NAME")
	prevkey := os.Getenv("DATASET_OBJECT_KEY")
	currkey := os.Getenv("PREPROCESSED_OBJECT_KEY")

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

	l, _ := client.ListObjectsV2(list_objects)

	type ob []map[string]string
	var jsonMap map[string]ob
	var ob_keys []string

	jsonBytes, _ := json.MarshalIndent(l, " ", " ")
	json.Unmarshal(jsonBytes, &jsonMap)
	objects := jsonMap["Contents"]

	for _, v := range objects {
		ob_keys = append(ob_keys, v["Key"])
	}

	prevObjectPresent := false
	currObjectPresent := false

	for _, obj := range ob_keys {
		if obj == prevkey {
			prevObjectPresent = true
		}
		if obj == currkey {
			currObjectPresent = true
		}
	}

	if !prevObjectPresent {
		log.Fatalln("Dataset Object not present in Cloud Object Storage")
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	if currObjectPresent {
		log.Fatalln("Preprocessed Object already present in Cloud Object Storage")
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	// users will need to create bucket, key (flat string name)
	Input := s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prevkey),
	}

	// Call Function
	res, _ := client.GetObject(&Input)

	body, _ := ioutil.ReadAll(res.Body)

	data := string(body)

	csvFile, err := os.Create(prevkey)

	if err != nil {
		log.Fatalln("Failed to create file: ", err)
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	_, err = csvFile.WriteString(data)

	if err != nil {
		log.Fatalln("Failed to write file: ", err)
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	preprocess(prevkey)

	DataBytes, erri := ioutil.ReadFile(currkey)

	if erri != nil {
		fmt.Printf("Failed opening file, error: %s", erri)
		os.Exit(1)
	}

	content := bytes.NewReader([]byte(DataBytes))

	input := s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(currkey),
		Body:   content,
	}

	// Call Function to upload (Put) an object
	result, _ := client.PutObject(&input)
	if result != nil {
		fmt.Println("Preprocessed Object pushed to Cloud Object Storage")
	}
}
