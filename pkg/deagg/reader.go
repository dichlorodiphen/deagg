package deagg

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type Reader struct {
	client     *kinesis.Client
	streamName string
}

func NewReader(streamName string) (*Reader, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	client := kinesis.NewFromConfig(cfg)
	return &Reader{
		client,
		streamName,
	}, nil

}

func (r *Reader) Read(seq, shardId string) ([]byte, error) {
	iter, err := r.client.GetShardIterator(context.TODO(), &kinesis.GetShardIteratorInput{
		StreamName:             &r.streamName,
		ShardId:                &shardId,
		ShardIteratorType:      types.ShardIteratorTypeAtSequenceNumber,
		StartingSequenceNumber: &seq,
	})
	if err != nil {
		return nil, err
	}

	output, err := r.client.GetRecords(context.TODO(), &kinesis.GetRecordsInput{
		ShardIterator: iter.ShardIterator,
		Limit:         aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}

	return output.Records[0].Data, nil
}
