package main

import (
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// init dependencies
	respParser := NewRESP()
	memory := NewMemory()
	processor := NewProcessor(respParser, memory)

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
		output, err := processor.Accept(buf)
		if err != nil {
			fmt.Println("Invalid command: ", err)
			break
		}
		conn.Write(output)
	}
}
