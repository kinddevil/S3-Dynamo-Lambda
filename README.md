# S3-Dynamo-Lambda
Simple lambda function to write S3 log into DynamoDB
## build
```
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags '-s -w' -a -installsuffix cgo main.go
```
## create zip file
```
zip function.zip main
```
## upload zip file to lambda directly or to S3

