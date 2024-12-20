package main

import (
	"fmt"
	"strconv"
)

type RESPType byte

const CR = '\r'
const LF = '\n'

const (
	SimpleError  RESPType = '-'
	SimpleString RESPType = '+'
	BulkString   RESPType = '$'
	Arrays       RESPType = '*'
	Integers     RESPType = ':'
)

type RespParser struct {
}

type RESP struct {
	Type   RESPType
	Nested []*RESP
	Data   []byte
	Origin []byte
}

func NewRESP() RespParser {
	return RespParser{}
}

// serializer
func (resp *RespParser) Serialize(input *RESP) []byte {
	builder := make([]byte, 0)
	switch input.Type {
	case SimpleError:
		builder = append(builder, resp.serialize_simple_error(input)...)
	case SimpleString:
		builder = append(builder, resp.serialize_string(input)...)
	case BulkString:
		builder = append(builder, resp.serialize_bulkString(input)...)
	case Arrays:
		builder = append(builder, resp.serialize_arrays(input)...)
	case Integers:
		builder = append(builder, resp.serialize_integers(input)...)
	}
	return builder
}

func (resp *RespParser) serialize_simple_error(input *RESP) []byte {
	builder := make([]byte, 0)
	builder = append(builder, byte(SimpleError))
	builder = append(builder, input.Data...)
	builder = append(builder, CR, LF)
	return builder
}

func (resp *RespParser) serialize_string(input *RESP) []byte {
	builder := make([]byte, 0)
	builder = append(builder, byte(SimpleString))
	builder = append(builder, input.Data...)
	builder = append(builder, CR, LF)
	return builder
}

func (resp *RespParser) serialize_bulkString(input *RESP) []byte {
	// null bulk string will be preresent as -1 in size
	lenStr := len(input.Data)
	if lenStr == 0 {
		lenStr = -1
	}

	builder := make([]byte, 0)
	builder = append(builder, byte(BulkString))
	builder = append(builder, []byte(fmt.Sprintf("%v", lenStr))...)
	builder = append(builder, CR, LF)

	if len(input.Data) > 0 {
		builder = append(builder, input.Data...)
		builder = append(builder, CR, LF)
	}

	return builder
}

func (resp *RespParser) serialize_arrays(input *RESP) []byte {
	builder := make([]byte, 0)
	builder = append(builder, byte(Arrays))
	builder = append(builder, []byte(fmt.Sprintf("%v", len(input.Nested)))...)
	builder = append(builder, CR, LF)
	for _, nested := range input.Nested {
		builder = append(builder, resp.Serialize(nested)...)
	}
	return builder
}

func (resp *RespParser) serialize_integers(input *RESP) []byte {
	builder := make([]byte, 0)
	builder = append(builder, byte(Integers))
	builder = append(builder, []byte(input.Data)...)
	builder = append(builder, CR, LF)
	return builder
}

// deserializer
func (resp *RespParser) Deserialize(input []byte) (*RESP, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("parse nil input")
	}
	respType, err := resp.getType(input[0])
	if err != nil {
		return nil, err
	}
	var value *RESP
	switch respType {
	case SimpleError:
		value, err = resp.deserialize_error(input[1:])
		if err != nil {
			return nil, err
		}
	case SimpleString:
		value, err = resp.deserialize_string(input[1:])
		if err != nil {
			return nil, err
		}
	case BulkString:
		value, err = resp.deserialize_bulkString(input[1:])
		if err != nil {
			return nil, err
		}
	case Arrays:
		value, err = resp.deserialize_arrays(input[1:])
		if err != nil {
			return nil, err
		}
	case Integers:
		value, err = resp.deserialize_integers(input[1:])
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("resp type invalid")
	}
	value.Origin = append(input[:1], value.Origin...)
	return value, nil
}

func (resp *RespParser) deserialize_error(input []byte) (*RESP, error) {
	i := 0
	for i < len(input)-1 && input[i] != CR && input[i+1] != LF {
		i++
	}
	if input[i] != CR || input[i+1] != LF {
		return nil, fmt.Errorf("invalid format for string type")
	}
	return &RESP{
		Type:   SimpleError,
		Nested: nil,
		Data:   input[:i],
		Origin: input[:i],
	}, nil
}

func (resp *RespParser) deserialize_string(input []byte) (*RESP, error) {
	i := 0
	for i < len(input)-1 && input[i] != CR && input[i+1] != LF {
		i++
	}
	if input[i] != CR || input[i+1] != LF {
		return nil, fmt.Errorf("invalid format for string type")
	}
	return &RESP{
		Type:   SimpleString,
		Nested: nil,
		Data:   input[:i],
		Origin: input[:i],
	}, nil
}

func (resp *RespParser) deserialize_bulkString(input []byte) (*RESP, error) {
	respSize, err := resp.deserialize_string(input)
	if err != nil {
		return nil, fmt.Errorf("invalid format for bulk string type - can't get length")
	}
	read := len(respSize.Data) + 2
	size, err := strconv.Atoi(string(respSize.Data))
	if err != nil {
		return nil, err
	}
	if len(input[read:]) < size+2 {
		return nil, fmt.Errorf("invalid format for bulk string type - size mismatched")
	}
	return &RESP{
		Type:   BulkString,
		Nested: nil,
		Data:   input[read : read+size],
		Origin: input[:read+size],
	}, nil
}

func (resp *RespParser) deserialize_arrays(input []byte) (*RESP, error) {
	respSize, err := resp.deserialize_string(input)
	if err != nil {
		return nil, fmt.Errorf("invalid format for bulk string type - can't get length")
	}
	read := len(respSize.Origin) + 2
	size, err := strconv.Atoi(string(respSize.Data))
	if err != nil {
		return nil, err
	}
	nested := make([]*RESP, 0)
	for size > 0 {
		respEle, err := resp.Deserialize(input[read:])
		if err != nil {
			return nil, err
		}
		nested = append(nested, respEle)
		read += len(respEle.Origin) + 2
		size -= 1
	}
	read -= 2
	return &RESP{
		Type:   Arrays,
		Nested: nested,
		Data:   input[:read],
		Origin: input[:read],
	}, nil
}

func (resp *RespParser) deserialize_integers(input []byte) (*RESP, error) {
	i := 0
	for i < len(input)-1 && input[i] != CR && input[i+1] != LF {
		i++
	}
	if input[i] != CR || input[i+1] != LF {
		return nil, fmt.Errorf("invalid format for string type")
	}
	return &RESP{
		Type:   Integers,
		Nested: nil,
		Data:   input[:i],
		Origin: input[:i],
	}, nil
}

// helpers
func (resp *RespParser) getType(char byte) (t RESPType, err error) {
	switch rune(char) {
	case '-':
		t = SimpleError
	case '+':
		t = SimpleString
	case '$':
		t = BulkString
	case '*':
		t = Arrays
	case ':':
		t = Integers
	default:
		err = fmt.Errorf("type not found: %c", rune(char))
	}
	return t, err
}
