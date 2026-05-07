package pbutil

import "google.golang.org/protobuf/proto"

func MustMarshal(m proto.Message) []byte {
	data, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

func MustUnmarshal(b []byte, m proto.Message) {
	err := proto.Unmarshal(b, m)
	if err != nil {
		panic(err)
	}
}
