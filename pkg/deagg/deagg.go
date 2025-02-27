package deagg

import (
	"bytes"
	"errors"

	"google.golang.org/protobuf/proto"
)

var magic = []byte{0xF3, 0x89, 0x9A, 0xC2}

const CHECKSUM_LEN = 16

func Deaggregate(data []byte) ([][]byte, error) {
	if !bytes.Equal(data[0:len(magic)], magic) {
		return nil, errors.New("expected the first four bytes to match the KCL magic number")
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
