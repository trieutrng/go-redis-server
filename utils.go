package main

import (
	"strings"
)

func RespTypeString(respType RESPType) string {
	switch respType {
	case SimpleString:
		return "SimpleString"
	case BulkString:
		return "BulkString"
	case Arrays:
		return "Arrays"
	}
	return "Not found"
}

func ToLowerCase(input string) string {
	return strings.ToLower(input)
}
