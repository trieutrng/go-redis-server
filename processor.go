package main

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type Executor func(resp *RESP) (*RESP, error)

type Processor struct {
	parser    RespParser
	memory    *Memory
	executors map[string]Executor
}

func NewProcessor(respParser RespParser, memory *Memory) *Processor {
	return &Processor{
		parser:    respParser,
		memory:    memory,
		executors: initExecutors(memory),
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

func initExecutors(memory *Memory) map[string]Executor {
	return map[string]Executor{
		"PING":     ping(),
		"ECHO":     echo(),
		"GET":      get(memory),
		"SET":      set(memory),
		"INFO":     info(),
		"REPLCONF": replConf(),
		"PSYNC":    psync(),
		"TYPE":     typeCmd(memory),
		"XADD":     xadd(memory),
	}
}

func ping() Executor {
	return func(resp *RESP) (*RESP, error) {
		return &RESP{
			Type: SimpleString,
			Data: []byte("PONG"),
		}, nil
	}
}

func echo() Executor {
	return func(resp *RESP) (*RESP, error) {
		if len(resp.Nested) < 2 {
			return nil, fmt.Errorf("ECHO command error: input insufficient")
		}
		return &RESP{
			Type: BulkString,
			Data: resp.Nested[1].Data,
		}, nil
	}
}

func set(memory *Memory) Executor {
	return func(resp *RESP) (*RESP, error) {
		if len(resp.Nested) < 3 {
			return nil, fmt.Errorf("insufficient arguments for SET")
		}
		argKey, argVal := resp.Nested[1], resp.Nested[2]
		key, val := string(argKey.Data), string(argVal.Data)

		// build SET options
		opts := Option{}
		i := 3
		for i < len(resp.Nested) {
			argOpt := resp.Nested[i]
			opt := ToLowerCase(string(argOpt.Data))
			switch opt {
			case "px":
				if i == len(resp.Nested)-1 {
					return nil, fmt.Errorf("invalid PX argument - missing PX value")
				}
				argPXVal := string(resp.Nested[i+1].Data)
				pxVal, err := strconv.Atoi(argPXVal)
				if err != nil {
					return nil, fmt.Errorf("invalid PX argument - invalid PX value: %v", err)
				}
				opts.expiry = time.Duration(pxVal) * time.Millisecond
			}
			i++
		}

		memory.Put(key,
			Entry{Type: "string", Value: val},
			opts)

		return &RESP{
			Type: SimpleString,
			Data: []byte("OK"),
		}, nil
	}
}

func get(memory *Memory) Executor {
	return func(resp *RESP) (*RESP, error) {
		if len(resp.Nested) < 2 {
			return nil, fmt.Errorf("insufficient arguments for GET")
		}
		argKey := resp.Nested[1]
		key := string(argKey.Data)

		val := memory.Get(key)

		return &RESP{
			Type: BulkString,
			Data: []byte((val.Value).(string)),
		}, nil
	}
}

func info() Executor {
	return func(resp *RESP) (*RESP, error) {
		v := reflect.ValueOf(ReplicationServerInfo)
		t := reflect.TypeOf(ReplicationServerInfo)
		replInfo := ""
		for i := 0; i < v.NumField(); i++ {
			infoTag := v.Type().Field(i).Tag.Get("info")
			fieldValue := v.FieldByName(t.Field(i).Name).Interface()
			replInfo = replInfo + string(CR) + string(LF) + fmt.Sprintf("%v:%v", infoTag, fieldValue)
		}
		return &RESP{
			Type: BulkString,
			Data: []byte(replInfo),
		}, nil
	}
}

func replConf() Executor {
	return func(resp *RESP) (*RESP, error) {
		return &RESP{
			Type: SimpleString,
			Data: []byte("OK"),
		}, nil
	}
}

func psync() Executor {
	return func(resp *RESP) (*RESP, error) {
		return &RESP{
			Type: SimpleString,
			Data: []byte(fmt.Sprintf("+FULLRESYNC %v %v", ReplicationServerInfo.MasterReplid, ReplicationServerInfo.MasterReplOffset)),
		}, nil
	}
}

func typeCmd(memory *Memory) Executor {
	return func(resp *RESP) (*RESP, error) {
		if len(resp.Nested) < 2 {
			return nil, fmt.Errorf("insufficient arguments for GET")
		}
		argKey := resp.Nested[1]
		key := string(argKey.Data)

		val := memory.Get(key)
		return &RESP{
			Type: SimpleString,
			Data: []byte(val.Type),
		}, nil
	}
}

func xadd(memory *Memory) Executor {
	return func(resp *RESP) (*RESP, error) {
		if len(resp.Nested) < 3 {
			return nil, fmt.Errorf("insufficient arguments for XADD")
		}
		argStreamKey := resp.Nested[1]
		key := string(argStreamKey.Data)
		entry := memory.Get(key)

		if entry.Type == "none" {
			var newEntry StreamEntry = make(map[string]map[string]string)
			entry = &Entry{
				Type:  "stream",
				Value: newEntry,
			}
		}

		stream := (entry.Value).(StreamEntry)
		id := string(resp.Nested[2].Data)

		if id == "*" || id[len(id)-1] == '*' {
			id = GenerateNextSeq(stream, id)
		} else {
			err := ValidateStreamId(stream, id)
			if err != nil {
				return &RESP{
					Type: SimpleError,
					Data: []byte(err.Error()),
				}, nil
			}
		}

		stream[id] = make(map[string]string)

		for i := 3; i < len(resp.Nested); i += 2 {
			key, value := string(resp.Nested[i].Data), string(resp.Nested[i+1].Data)
			stream[id][key] = value
		}

		memory.Put(key, *entry, Option{})

		return &RESP{
			Type: BulkString,
			Data: []byte(id),
		}, nil
	}
}
