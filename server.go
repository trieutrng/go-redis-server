package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/google/uuid"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type serverOption struct {
	port      string
	replicaOf string
}

func getServerOptions(args []string) serverOption {
	opts := serverOption{
		port: "6379",
	}
	for i, arg := range args {
		switch arg {
		case "--port":
			if i < len(args)-1 {
				opts.port = args[i+1]
			}
		case "--replicaof":
			opts.replicaOf = args[i+1]
		}
	}
	return opts
}

func main() {
	opts := getServerOptions(os.Args)

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", fmt.Sprintf(":%v", opts.port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// init dependencies
	respParser := NewRESP()
	memory := NewMemory()
	transaction := NewTransaction()
	processor := NewProcessor(respParser, memory, transaction)

	// process replication
	err = InitReplication(processor, opts)
	if err != nil {
		fmt.Println("Error when replicating: ", err.Error())
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handle(conn, processor)
	}
}

func handle(conn net.Conn, processor *Processor) {
	txId := uuid.New().String()
	txContext := context.WithValue(context.Background(), "txId", txId)
	buf := make([]byte, 1024)
	for {
		read, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error when parsing command!", err.Error())
			break
		}
		if read == 0 {
			fmt.Println("No data read")
			break
		}

		// deep copy to avoid referencing
		bufCmd := make([]byte, len(buf))
		copy(bufCmd, buf)

		output, err := processor.Accept(txContext, bufCmd)
		if err != nil {
			fmt.Println("Invalid command: ", err)
			break
		}
		conn.Write(output)
	}
}
