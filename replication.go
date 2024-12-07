package main

import (
	"fmt"
	"net"
	"strings"
	"time"
)

type redisReplicationInfo struct {
	Role                       string `info:"role"`
	ConnectedSlaves            int    `info:"connected_slaves"`
	MasterReplid               string `info:"master_replid"`
	MasterReplOffset           int    `info:"master_repl_offset"`
	SecondReplOffset           int    `info:"second_repl_offset"`
	ReplBacklogActive          int    `info:"repl_backlog_active"`
	ReplBacklogSize            int    `info:"repl_backlog_size"`
	ReplBacklogFirstByteOffset int    `info:"repl_backlog_first_byte_offset"`
	ReplBacklogHistlen         int    `info:"repl_backlog_histlen"`
}

var ReplicationServerInfo redisReplicationInfo

func InitReplication(procesor *Processor, opts serverOption) error {
	ReplicationServerInfo = redisReplicationInfo{
		Role:                       "master",
		ConnectedSlaves:            0,
		MasterReplid:               "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", // hard coded
		MasterReplOffset:           0,
		SecondReplOffset:           -1,
		ReplBacklogActive:          0,
		ReplBacklogSize:            1048576,
		ReplBacklogFirstByteOffset: 0,
		ReplBacklogHistlen:         0,
	}

	if len(opts.replicaOf) > 0 {
		ReplicationServerInfo.Role = "slave"
		err := handshake(procesor, opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func handshake(procesor *Processor, opts serverOption) error {
	masterAddr := strings.Split(opts.replicaOf, " ")
	conn, err := connectMaster(masterAddr[0], masterAddr[1])
	if err != nil {
		return err
	}
	defer conn.Close()

	// step 1: PING the master
	// -> PING
	pingResp := &RESP{
		Type: Arrays,
		Nested: []*RESP{
			{
				Type: BulkString,
				Data: []byte("PING"),
			},
		},
	}
	pingMsg := procesor.parser.Serialize(pingResp)
	fmt.Printf("SLAVE_REQUEST: %v", string(pingMsg))

	data, err := request(conn, pingMsg)
	if err != nil {
		return err
	}
	fmt.Printf("MASTER_RESPOND: %v", string(data))

	// step 2: send REPLCONF with slave address
	// REPLCONF listening-port <PORT>
	replConfAddr := &RESP{
		Type: Arrays,
		Nested: []*RESP{
			{
				Type: BulkString,
				Data: []byte("REPLCONF"),
			},
			{
				Type: BulkString,
				Data: []byte("listening-port"),
			},
			{
				Type: BulkString,
				Data: []byte(opts.port),
			},
		},
	}
	replConfAddrMsg := procesor.parser.Serialize(replConfAddr)
	fmt.Printf("SLAVE_REQUEST: %v", string(replConfAddrMsg))

	data, err = request(conn, replConfAddrMsg)
	if err != nil {
		return err
	}
	fmt.Printf("MASTER_RESPOND: %v", string(data))

	// step 3: send REPLCONF with capa psync
	// -> REPLCONF capa psync2
	replConfPsync := &RESP{
		Type: Arrays,
		Nested: []*RESP{
			{
				Type: BulkString,
				Data: []byte("REPLCONF"),
			},
			{
				Type: BulkString,
				Data: []byte("capa"),
			},
			{
				Type: BulkString,
				Data: []byte("psync2"),
			},
		},
	}
	replConfPsycnMsg := procesor.parser.Serialize(replConfPsync)
	fmt.Printf("SLAVE_REQUEST: %v", string(replConfPsycnMsg))

	data, err = request(conn, replConfPsycnMsg)
	if err != nil {
		return err
	}
	fmt.Printf("MASTER_RESPOND: %v", string(data))

	// step 4: send PSYNC
	// -> PSYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb -1
	psync := &RESP{
		Type: Arrays,
		Nested: []*RESP{
			{
				Type: BulkString,
				Data: []byte("PSYNC"),
			},
			{
				Type: BulkString,
				Data: []byte("?"),
			},
			{
				Type: BulkString,
				Data: []byte("-1"),
			},
		},
	}
	psyncMsg := procesor.parser.Serialize(psync)
	fmt.Printf("SLAVE_REQUEST: %v", string(psyncMsg))

	data, err = request(conn, psyncMsg)
	if err != nil {
		return err
	}
	fmt.Printf("MASTER_RESPOND: %v", string(data))

	return nil
}

func request(conn net.Conn, data []byte) ([]byte, error) {
	// send request
	_, err := conn.Write(data)
	if err != nil {
		return nil, err
	}

	// waiting for the response
	buf := make([]byte, 4096)
	_, err = conn.Read(buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func connectMaster(host, port string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 5*time.Second)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
