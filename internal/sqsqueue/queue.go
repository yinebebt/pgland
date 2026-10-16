// Package sqsqueue publishes and receives sync run ids.
// Endpoint is a LocalStack gateway in this lab and SQS in production.
package sqsqueue

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// MaxReceives matches the queue redrive policy. The last failed receive is left
// on the queue so SQS moves it to the dead-letter queue.
const MaxReceives = 3

// Delivery is one received sync run.
type Delivery struct {
	RunID        string
	Receipt      string
	ReceiveCount int
}

// Queue is the main sync queue.
type Queue struct {
	client *sqs.Client
	url    string
}

// Config is the queue endpoint and names.
type Config struct {
	Endpoint          string
	Region            string
	AccessKey         string
	SecretKey         string
	QueueName         string
	DLQName           string
	VisibilitySeconds int
}

// Open creates the sync queue and its dead-letter queue.
func Open(ctx context.Context, cfg Config) (*Queue, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
	})

	dlqURL, err := create(ctx, client, cfg.DLQName, nil)
	if err != nil {
		return nil, err
	}
	dlqURL = withEndpointHost(cfg.Endpoint, dlqURL)
	arnOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return nil, fmt.Errorf("get dlq arn: %w", err)
	}
	arn := arnOut.Attributes[string(types.QueueAttributeNameQueueArn)]
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"%d"}`, arn, MaxReceives)
	queueURL, err := create(ctx, client, cfg.QueueName, map[string]string{
		"VisibilityTimeout": strconv.Itoa(cfg.VisibilitySeconds),
		"RedrivePolicy":     redrive,
	})
	if err != nil {
		return nil, err
	}
	return &Queue{client: client, url: withEndpointHost(cfg.Endpoint, queueURL)}, nil
}

func create(ctx context.Context, client *sqs.Client, name string, attrs map[string]string) (string, error) {
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	if err != nil {
		return "", fmt.Errorf("create queue %s: %w", name, err)
	}
	if out.QueueUrl == nil || *out.QueueUrl == "" {
		return "", fmt.Errorf("create queue %s: empty url", name)
	}
	return *out.QueueUrl, nil
}

// withEndpointHost keeps the queue path and uses the configured endpoint host.
// LocalStack often returns localhost.localstack.cloud, which this process cannot call.
func withEndpointHost(endpoint, queueURL string) string {
	base, err := url.Parse(endpoint)
	got, err2 := url.Parse(queueURL)
	if err != nil || err2 != nil || base.Host == "" || got.Host == "" {
		return queueURL
	}
	got.Scheme = base.Scheme
	got.Host = base.Host
	return got.String()
}

// Enqueue publishes a sync run id.
func (q *Queue) Enqueue(ctx context.Context, runID string) error {
	_, err := q.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.url),
		MessageBody: aws.String(runID),
	})
	if err != nil {
		return fmt.Errorf("enqueue sync: %w", err)
	}
	return nil
}

// Receive waits up to 20 seconds for one run.
func (q *Queue) Receive(ctx context.Context) (Delivery, bool, error) {
	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(q.url),
		MaxNumberOfMessages:         1,
		WaitTimeSeconds:             20,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return Delivery{}, false, fmt.Errorf("receive sync: %w", err)
	}
	if len(out.Messages) == 0 || out.Messages[0].Body == nil || out.Messages[0].ReceiptHandle == nil {
		return Delivery{}, false, nil
	}
	m := out.Messages[0]
	count := 1
	if raw := m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			count = n
		}
	}
	return Delivery{RunID: *m.Body, Receipt: *m.ReceiptHandle, ReceiveCount: count}, true, nil
}

// Delete acknowledges a finished run.
func (q *Queue) Delete(ctx context.Context, receipt string) error {
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: aws.String(receipt),
	})
	if err != nil {
		return fmt.Errorf("delete sync message: %w", err)
	}
	return nil
}
