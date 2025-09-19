package pageproto

import (
	"google.golang.org/protobuf/proto"

	"github.com/example/wikipedia-parser/proto"
)

type Page = pb.Page

func Marshal(p *Page) ([]byte, error) {
	return proto.Marshal(p)
}

func Unmarshal(data []byte) (*Page, error) {
	var msg pb.Page
	if err := proto.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
