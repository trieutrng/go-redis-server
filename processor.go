package main

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
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

	query := strings.ToUpper(string(resp.Nested[0].Data))
	executor, ok := p.executors[query]
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
		"XRANGE":   xrange(memory),
		"XREAD":    xread(memory),
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

func xrange(memory *Memory) Executor {
	return func(resp *RESP) (*RESP, error) {
		if len(resp.Nested) < 4 {
			return nil, fmt.Errorf("insufficient arguments for XRANGE")
		}

		argStreamKey := resp.Nested[1]
		key := string(argStreamKey.Data)

		entry := memory.Get(key)

		if entry.Type == "none" {
			return nil, fmt.Errorf("stream with key %v not found", key)
		}

		startArg, endArg := resp.Nested[2], resp.Nested[3]
		start, end := string(startArg.Data), string(endArg.Data)

		result := &RESP{
			Type:   Arrays,
			Nested: make([]*RESP, 0),
		}
		stream := (entry.Value).(StreamEntry)
		keyRange := QueryStreamKeysByRange(stream, start, end, true)

		for _, key := range keyRange {
			// for the item key
			itemKeyResp := &RESP{
				Type: BulkString,
				Data: []byte(key),
			}
			itemValueResp := &RESP{
				Type:   Arrays,
				Nested: make([]*RESP, 0),
			}

			// for the item values
			item := stream[key]
			for k, v := range item {
				keyResp := &RESP{
					Type: BulkString,
					Data: []byte(k),
				}
				valueResp := &RESP{
					Type: BulkString,
					Data: []byte(v),
				}
				itemValueResp.Nested = append(itemValueResp.Nested, keyResp, valueResp)
			}

			itemResp := &RESP{
				Type: Arrays,
				Nested: []*RESP{
					itemKeyResp,
					itemValueResp,
				},
			}
			result.Nested = append(result.Nested, itemResp)
		}

		return result, nil
	}
}

func xread(memory *Memory) Executor {
	return func(resp *RESP) (*RESP, error) {
		if len(resp.Nested) < 4 {
			return nil, fmt.Errorf("insufficient arguments for XREAD")
		}

		// process options
		isBlocking, blockingTime := false, 0
		i := 1
		for i < len(resp.Nested) {
			if string(resp.Nested[i].Data) == "streams" {
				i += 1
				break
			}
			if string(resp.Nested[i].Data) == "block" {
				isBlocking = true
				blockingTime, _ = strconv.Atoi(string(resp.Nested[i+1].Data))
				i += 2
				continue
			}
			i += 1
		}

		streams := make(map[string]string)
		numStream := int((len(resp.Nested) - i) / 2)

		for j := i; j < len(resp.Nested)-numStream; j++ {
			streams[string(resp.Nested[j].Data)] = string(resp.Nested[j+numStream].Data)
		}

		if isBlocking {
			if blockingTime > 0 {
				<-time.After(time.Duration(blockingTime) * time.Millisecond)
			} else {
				// block until there is update from the querying streams
				waitCtx, cancel := context.WithCancel(context.Background())
				updated, check := make(chan bool, 10), make(chan bool, 10)
				for streamId := range streams {
					entry := memory.Get(streamId)
					stream := (entry.Value).(StreamEntry)

					// for each stream, continuing check if there is any updates
					go func(ctx context.Context, stream StreamEntry, check chan bool, updated chan bool, oldLen int) {
						for {
							select {
							case <-ctx.Done():
								return
							case <-time.After(time.Duration(10) * time.Millisecond):
								check <- true
							case <-check:
								if len(stream) > oldLen {
									updated <- true
								}
							}
						}
					}(waitCtx, stream, check, updated, len(stream))
				}

				// wait until there is any update signal
				<-updated
				// cancel all the goroutine
				cancel()
			}
		}

		output := &RESP{
			Type:   Arrays,
			Nested: make([]*RESP, 0),
		}

		// build output
		for streamId, boundId := range streams {
			entry := memory.Get(streamId)
			stream := (entry.Value).(StreamEntry)
			keyRange := QueryStreamKeysByRange(stream, boundId, "+", false)

			streamItemResp := &RESP{
				Type:   Arrays,
				Nested: make([]*RESP, 0),
			}

			// for every item in stream
			for _, key := range keyRange {
				// for the item key
				itemKeyResp := &RESP{
					Type: BulkString,
					Data: []byte(key),
				}
				itemValueResp := &RESP{
					Type:   Arrays,
					Nested: make([]*RESP, 0),
				}

				// for the item values
				item := stream[key]
				for k, v := range item {
					keyResp := &RESP{
						Type: BulkString,
						Data: []byte(k),
					}
					valueResp := &RESP{
						Type: BulkString,
						Data: []byte(v),
					}
					itemValueResp.Nested = append(itemValueResp.Nested, keyResp, valueResp)
				}

				streamItemResp.Nested = append(streamItemResp.Nested, &RESP{
					Type: Arrays,
					Nested: []*RESP{
						itemKeyResp,
						itemValueResp,
					},
				})
			}

			// build output
			streamIdResp := &RESP{
				Type: BulkString,
				Data: []byte(streamId),
			}
			streamQueryResp := &RESP{
				Type: Arrays,
				Nested: []*RESP{
					streamIdResp,
					streamItemResp,
				},
			}
			if len(streamItemResp.Nested) > 0 {
				output.Nested = append(output.Nested, streamQueryResp)
			}
		}

		if len(output.Nested) == 0 {
			return &RESP{
				Type: BulkString,
				Data: []byte(""),
			}, nil
		}

		return output, nil
	}
}
