package main

import (
	"testing"
)

func TestRespParser_Deserialize(t *testing.T) {
	testcases := []struct {
		name     string
		args     []byte
		expected RESPType
	}{
		{
			name:     "Deserialize 1",
			args:     []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
			expected: Arrays,
		},
		{
			name:     "Deserialize 2",
			args:     []byte("$3\r\nSET\r\n"),
			expected: BulkString,
		},
		{
			name:     "Deserialize 3",
			args:     []byte("+PING\r\n"),
			expected: SimpleString,
		},
		{
			name:     "Deserialize 4",
			args:     []byte(":51\r\n"),
			expected: Integers,
		},
		{
			name:     "Deserialize 5",
			args:     []byte("-ERR value is not an integer or out of range\r\n"),
			expected: SimpleError,
		},
	}

	respParser := NewRESP()
	for _, tt := range testcases {
		output, err := respParser.Deserialize(tt.args)
		if err != nil {
			t.Errorf("test: %v - expected no error, but got: %v", tt.name, err)
		}
		if output.Type != tt.expected {
			t.Errorf("test: %v \n- expected arrays \n- actual: %v", tt.name, RespTypeString(output.Type))
		}
	}
}

func TestRespParser_Serialize(t *testing.T) {
	testcases := []struct {
		name     string
		args     *RESP
		expected string
	}{
		{
			name: "Serialize 1",
			args: &RESP{
				Type: Arrays,
				Nested: []*RESP{
					{
						Type: BulkString,
						Data: []byte("SET"),
					},
					{
						Type: BulkString,
						Data: []byte("Person"),
					},
					{
						Type: BulkString,
						Data: []byte("TrieuTruong"),
					},
				},
			},
			expected: "*3\r\n$3\r\nSET\r\n$6\r\nPerson\r\n$11\r\nTrieuTruong\r\n",
		},
		{
			name: "Serialize 2",
			args: &RESP{
				Type: SimpleString,
				Data: []byte("PONGPING"),
			},
			expected: "+PONGPING\r\n",
		},
		{
			name: "Serialize 2",
			args: &RESP{
				Type: BulkString,
				Data: []byte("PONGPING-PINGPONG"),
			},
			expected: "$17\r\nPONGPING-PINGPONG\r\n",
		},
	}

	respParser := NewRESP()
	for _, tt := range testcases {
		output := respParser.Serialize(tt.args)
		if string(output) != tt.expected {
			t.Errorf("test: %v \n- expected: %v \n- actual: %v", tt.name, tt.expected, string(output))
		}
	}
}
