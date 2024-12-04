package main

import "fmt"

const (
	Ping = "PING"
	Echo = "ECHO"
)

type Executor func(resp *RESP) (*RESP, error)

type Processor struct {
	parser    RespParser
	executors map[string]Executor
}

func NewProcessor(respParser RespParser) *Processor {
	return &Processor{
		parser:    respParser,
		executors: initExecutors(),
	}
}

func (p *Processor) Accept(cmd []byte) ([]byte, error) {
	resp, err := p.parser.Deserialize(cmd)
	if err != nil {
		return nil, err
	}
	if resp.Type != Arrays {
		return nil, fmt.Errorf("expected Arrays type for command, but received: %v", RespTypeString(resp.Type))
	}
	if len(resp.Nested) == 0 {
		return nil, fmt.Errorf("invalid command: command empty")
	}

	executor, ok := p.executors[string(resp.Nested[0].Data)]
	if !ok {
		return nil, fmt.Errorf("command not supported")
	}

	output, err := executor(resp)
	if err != nil {
		return nil, err
	}

	return p.parser.Serialize(output), nil
}

func initExecutors() map[string]Executor {
	return map[string]Executor{
		"PING": ping,
		"ECHO": echo,
	}
}

func ping(resp *RESP) (*RESP, error) {
	return &RESP{
		Type: SimpleString,
		Data: []byte("PONG"),
	}, nil
}

func echo(resp *RESP) (*RESP, error) {
	if len(resp.Nested) < 2 {
		return nil, fmt.Errorf("ECHO command error: input insufficient")
	}
	return &RESP{
		Type: BulkString,
		Data: resp.Nested[1].Data,
	}, nil
}
