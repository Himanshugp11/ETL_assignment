package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	uuid "github.com/satori/go.uuid"
)

func fail(err error) {
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(-1)
	}
}

const (
	message1 = `{
  "id": 3,
  "mail": "aaa@gmail.com",
  "name": "Mahmoud",
  "surname": "Mahmoudi",
  "route": [
    {
      "from": "B",
      "to": "C",
      "duration": 15,
      "started_at": "10/10/2022 11:10:00"
    },
    {
      "from": "C",
      "to": "E",
      "duration": 10,
      "started_at": "10/10/2022 11:17:15"
    }
  ]
}`
	message2 = `{
  "id": 5,
  "mail": "mmm@nocompany.com",
  "name": "Kacper",
  "surname": "Kacperian",
  "locations": [
    {
      "location": "F",
      "timestamp": 1667999699
    },
    {
      "location": "G",
      "timestamp": 1668975653
    }
  ]
}`
	message3 = `{
  "id": 6,
  "mail": "kkk@nocompany.com",
  "name": "Tulga",
  "surname": "Khan",
  "age": "25",
  "workplace": "home",
  "locations": [
    {
      "location": "A",
      "timestamp": 1667975699
    },
    {
      "location": "B",
      "timestamp": 1667975893
    }
  ]
}`
	message4 = `{
  "id": 6,
  "mail": "kkk@nocompany.com",
  "name": "Tulga",
  "surname": "Khan",
  "age": "25",
  "workplace": "office1",
  "locations": [
    {
      "location": "A",
      "timestamp": 1667975699
    },
    {
      "location": "B",
      "timestamp": 1667975893
    }
  ]
}`
)

func main() {

	s, err := getSqsService()
	fail(err)

	queueName := "test-queue"

	fmt.Printf("Creating SQS queue [%s]\n", queueName)
	output, err := s.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	fail(err)
	fmt.Printf("Queue created, url: %s\n", *output.QueueUrl)

	entries := []*sqs.SendMessageBatchRequestEntry{
		messageEntry("malformed"),
		messageEntry(message1),
		messageEntry(message2),
		messageEntry(message3),
		messageEntry(message4),
	}
	fmt.Println("Sending messages to the queue...")
	sendOutput, err := s.SendMessageBatch(&sqs.SendMessageBatchInput{Entries: entries, QueueUrl: output.QueueUrl})
	fail(err)

	if len(sendOutput.Failed) > 0 {
		for _, e := range sendOutput.Failed {
			fmt.Printf("Message failed to be sent, reason: %s\n", *(e.Message))
		}
		fail(fmt.Errorf("%d messages failed to be sent", len(sendOutput.Failed)))
	}

}

func messageEntry(body string) *sqs.SendMessageBatchRequestEntry {
	var delay int64 = 1
	id := uuid.NewV4().String()
	return &sqs.SendMessageBatchRequestEntry{
		DelaySeconds: aws.Int64(delay),
		MessageBody:  aws.String(body),
		Id:           aws.String(id),
	}
}

func getSqsService() (*sqs.SQS, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("ap-south-1"),
		Endpoint: aws.String("http://localhost:4566"),
	})
	if err != nil {
		return nil, err
	}
	return sqs.New(sess), nil
}
