package pageproto

import (
	"sort"

	"google.golang.org/protobuf/encoding/protowire"
)

type Page struct {
	SrcURL   string
	HTTPCode uint32
	Headers  map[string]string
	Content  []byte
	IP       uint32
}

func Marshal(p Page) ([]byte, error) {
	var out []byte
	if p.SrcURL != "" {
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendString(out, p.SrcURL)
	}
	out = protowire.AppendTag(out, 2, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(p.HTTPCode))

	if len(p.Headers) > 0 {
		keys := make([]string, 0, len(p.Headers))
		for k := range p.Headers {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := p.Headers[k]
			entry := []byte{}
			entry = protowire.AppendTag(entry, 1, protowire.BytesType)
			entry = protowire.AppendString(entry, k)
			entry = protowire.AppendTag(entry, 2, protowire.BytesType)
			entry = protowire.AppendString(entry, v)
			out = protowire.AppendTag(out, 3, protowire.BytesType)
			out = protowire.AppendBytes(out, entry)
		}
	}

	if len(p.Content) > 0 {
		out = protowire.AppendTag(out, 4, protowire.BytesType)
		out = protowire.AppendBytes(out, p.Content)
	}

	out = protowire.AppendTag(out, 5, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(p.IP))
	return out, nil
}

func Unmarshal(data []byte) (Page, error) {
	var p Page
	headers := make(map[string]string)
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return Page{}, protowire.ParseError(n)
		}
		data = data[n:]
		var err error
		switch num {
		case 1:
			var v []byte
			v, data, err = consumeBytes(data)
			if err != nil {
				return Page{}, err
			}
			p.SrcURL = string(v)
		case 2:
			var v uint64
			v, data, err = consumeVarint(data)
			if err != nil {
				return Page{}, err
			}
			p.HTTPCode = uint32(v)
		case 3:
			var entry []byte
			entry, data, err = consumeBytes(data)
			if err != nil {
				return Page{}, err
			}
			key, val, err := decodeHeader(entry)
			if err != nil {
				return Page{}, err
			}
			headers[key] = val
		case 4:
			var v []byte
			v, data, err = consumeBytes(data)
			if err != nil {
				return Page{}, err
			}
			p.Content = append([]byte(nil), v...)
		case 5:
			var v uint64
			v, data, err = consumeVarint(data)
			if err != nil {
				return Page{}, err
			}
			p.IP = uint32(v)
		default:
			consumed := protowire.ConsumeFieldValue(num, typ, data)
			if consumed < 0 {
				return Page{}, protowire.ParseError(consumed)
			}
			data = data[consumed:]
		}
	}
	if len(headers) > 0 {
		p.Headers = headers
	}
	return p, nil
}

func decodeHeader(data []byte) (string, string, error) {
	var key, value string
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			v, rest, err := consumeBytes(data)
			if err != nil {
				return "", "", err
			}
			key = string(v)
			data = rest
		case 2:
			v, rest, err := consumeBytes(data)
			if err != nil {
				return "", "", err
			}
			value = string(v)
			data = rest
		default:
			consumed := protowire.ConsumeFieldValue(num, typ, data)
			if consumed < 0 {
				return "", "", protowire.ParseError(consumed)
			}
			data = data[consumed:]
		}
	}
	return key, value, nil
}

func consumeBytes(data []byte) ([]byte, []byte, error) {
	v, n := protowire.ConsumeBytes(data)
	if n < 0 {
		return nil, nil, protowire.ParseError(n)
	}
	return v, data[n:], nil
}

func consumeVarint(data []byte) (uint64, []byte, error) {
	v, n := protowire.ConsumeVarint(data)
	if n < 0 {
		return 0, nil, protowire.ParseError(n)
	}
	return v, data[n:], nil
}
