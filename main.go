package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var db = dynamodb.New(session.New(), aws.NewConfig().WithRegion("cn-northwest-1"))

type S3Records struct {
	Records []S3Record `json:"Records"`
}

type S3Record struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	EventTime         string            `json:"eventTime"`
	EventName         string            `json:"eventName"`
	RequestParameters RequestParameters `json:"requestParameters"`
	ResponseElements  ResponseElements  `json:"responseElements"`
	S3                S3Obj             `json:"s3"`
}

type RequestParameters struct {
	SourceIPAddress string `json:"sourceIPAddress"`
}

type ResponseElements struct {
	XAmzRequestId string `json:"x-amz-request-id"`
	XAmzId2       string `json:"x-amz-id-2"`
}

type S3Obj struct {
	S3SchemaVersion string      `json:"s3SchemaVersion"`
	ConfigurationId string      `json:"configurationId"`
	Bucket          Bucket      `json:"bucket"`
	Detail          S3ObjDetail `json:"object"`
}

type Bucket struct {
	Name          string        `json:"name"`
	Arn           string        `json:"arn"`
	OwnerIdentity OwnerIdentity `json:"ownerIdentity"`
}

type OwnerIdentity struct {
	PrincipalId string `json:"principalId"`
}

type S3ObjDetail struct {
	Key       string `json:"key"`
	Size      int32  `json:"size"`
	ETag      string `json:"eTag"`
	Sequencer string `json:"sequencer"`
}

type S3LogItem struct {
	Timestamp         string            `json:"id"`
	Key               string            `json:"key"`
	Size              int32             `json:"Size"`
	ETag              string            `json:"eTag"`
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	Sequencer         string            `json:"sequencer"`
	RequestParameters RequestParameters `json:"requestParameters"`
	Bucket            Bucket            `json:"bucket"`
}

func HandleRequest(ctx context.Context, records S3Records) (string, error) {

	cnt := 0
	for _, record := range records.Records {
		log.Println("Got S3 operation -> %v", record)
		item := &S3LogItem{
			Timestamp:         record.EventTime,
			Key:               record.S3.Detail.Key,
			Size:              record.S3.Detail.Size,
			ETag:              record.S3.Detail.ETag,
			EventVersion:      record.EventVersion,
			EventSource:       record.EventSource,
			AwsRegion:         record.AwsRegion,
			Sequencer:         record.S3.Detail.Sequencer,
			RequestParameters: record.RequestParameters,
			Bucket:            record.S3.Bucket,
		}

		if av, err := dynamodbattribute.MarshalMap(item); err != nil {
			log.Println("Error create Dynamo record of %v", item)
		} else {
			input := &dynamodb.PutItemInput{
				Item:      av,
				TableName: aws.String("s3log"),
			}
			if _, err := db.PutItem(input); err != nil {
				log.Println("Error insert Dynamo record of %v", item)
			} else {
				cnt += 1
			}
		}

	}
	ret := fmt.Sprintf("Finish %v s3 log items in %v total", cnt, len(records.Records))
	log.Println(ret)
	return ret, nil
}

func main() {
	lambda.Start(HandleRequest)
}
