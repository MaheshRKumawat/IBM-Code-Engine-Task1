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

	frq, errq := os.Create("Reduced.csv")
	frp, errp := os.Create("Top_Products_Price.csv")
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
	bucketName := os.Getenv("BUCKET_NAME")
	prevkey := os.Getenv("MAPPED_OBJECT_KEY")
	currkey1 := os.Getenv("REDUCED_OBJECT_KEY")
	currkey2 := os.Getenv("TOP_PRODUCTS_OBJECT_KEY")

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
	curr1_ObjectPresent := false
	curr2_ObjectPresent := false

	for _, obj := range ob_keys {
		if obj == prevkey {
			prevObjectPresent = true
		}
		if obj == currkey1 {
			curr1_ObjectPresent = true
		}
		if obj == currkey2 {
			curr2_ObjectPresent = true
		}
	}

	if !prevObjectPresent {
		log.Fatalln("Mapper Object not present in Cloud Object Storage")
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	if curr1_ObjectPresent {
		log.Fatalln("Reducer Object already present in Cloud Object Storage")
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	if curr2_ObjectPresent {
		log.Fatalln("Top Products Price Object already present in Cloud Object Storage")
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

	reduce(prevkey)

	reducer_topProducts := []string{currkey1, currkey2}

	for _, val := range reducer_topProducts {
		DataBytes, erri := ioutil.ReadFile(val)

		if erri != nil {
			fmt.Printf("Failed opening file, error: %s", erri)
			os.Exit(1)
		}

		content := bytes.NewReader([]byte(DataBytes))

		input := s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(val),
			Body:   content,
		}

		// Call Function to upload (Put) an object
		result, _ := client.PutObject(&input)
		if result != nil {
			fmt.Println("Object pushed to Cloud Object Storage")
		}
	}
}
