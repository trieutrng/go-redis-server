package main

import "testing"

func TestProcessor_Accept(t *testing.T) {
	testcases := []struct {
		name     string
		args     string
		expected string
	}{
		{
			name:     "processor accept 1",
			args:     "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
			expected: "$3\r\nhey\r\n",
		},
		{
			name:     "processor accept 2",
			args:     "*1\r\n$4\r\nPING\r\n",
			expected: "+PONG\r\n",
		},
		{
			name:     "processor accept 3",
			args:     "*2\r\n$4\r\nECHO\r\n$12\r\nTRIEU TRUONG\r\n",
			expected: "$12\r\nTRIEU TRUONG\r\n",
		},
		{
			name:     "processor accept 4",
			args:     "*5\r\n$3\r\nSET\r\n$5\r\nmango\r\n$5\r\napple\r\n$2\r\npx\r\n$3\r\n100\r\n",
			expected: "+OK\r\n",
		},
		{
			name:     "processor accept 5",
			args:     "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
			expected: "$-1\r\n",
		},
	}

	respParser := NewRESP()
	memory := NewMemory()
	processor := NewProcessor(respParser, memory)
	for _, tt := range testcases {
		output, err := processor.Accept([]byte(tt.args))
		if err != nil {
			t.Errorf("test: %v - unexpected error: %v", tt.name, err)
		}
		if string(output) != string(tt.expected) {
			t.Errorf("test: %v - expected: %v - actual: %v", tt.name, string(tt.expected), string(output))
		}
	}
}
