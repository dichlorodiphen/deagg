package deagg

import (
	"bytes"
	"errors"

	"google.golang.org/protobuf/proto"
)

var magic = []byte{0xF3, 0x89, 0x9A, 0xC2}

// ErrInvalidFormat is the error returned by deaggregate when the data is not in the KPL aggregation format.
var ErrInvalidFormat = errors.New("expected the first four bytes to match the KCL magic number")

const CHECKSUM_LEN = 16

func Deaggregate(data []byte) ([][]byte, error) {
	if !bytes.Equal(data[0:len(magic)], magic) {
		return nil, ErrInvalidFormat
	}

	var agg AggregatedRecord
	if err := proto.Unmarshal(data[len(magic):(len(data)-CHECKSUM_LEN)], &agg); err != nil {
		return nil, err
	}

	var records [][]byte
	for _, r := range agg.Records {
		records = append(records, r.GetData())
	}

	return records, nil
}

func DeaggregateOrNoOp(data []byte) ([][]byte, error) {
	records, err := Deaggregate(data)
	if err != nil {
		if err == ErrInvalidFormat {
			return [][]byte{data}, nil
		}
		return nil, err
	}
	return records, nil
}
