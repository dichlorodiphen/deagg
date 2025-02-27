package deagg

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type Reader struct {
	client *kinesis.Client
}

func NewReader(streamName string) (*kinesis.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	return kinesis.NewFromConfig()
}

func (r *Reader) Read(seq, shardId string) ([]byte, error) {

}
