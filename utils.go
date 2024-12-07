package main

import (
	"fmt"
	"sort"
	"strconv"
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

func ValidateStreamId(streamEntry StreamEntry, id string) error {
	lastTime, lastSeq := "0", "0"

	if len(streamEntry) > 0 {
		keys := make([]string, 0, len(streamEntry))
		for k := range streamEntry {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		latest := keys[len(keys)-1]

		splittedLatest := strings.Split(latest, "-")
		lastTime, lastSeq = splittedLatest[0], splittedLatest[1]
	}

	splittedNow := strings.Split(id, "-")
	time, seq := splittedNow[0], splittedNow[1]

	if time == "0" && seq == "0" {
		return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	if time < lastTime || time == lastTime && seq <= lastSeq {
		return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}

func GenerateNextSeq(streamEntry StreamEntry, id string) string {
	lastTime, lastSeq := "0", "0"
	if len(streamEntry) > 0 {
		keys := make([]string, 0, len(streamEntry))
		for k := range streamEntry {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		latest := keys[len(keys)-1]

		splittedLatest := strings.Split(latest, "-")
		lastTime, lastSeq = splittedLatest[0], splittedLatest[1]
	}

	if id == "*" {
		if lastTime == "0" && lastSeq == "0" {
			return "0-1"
		}
		lastSeqInt, _ := strconv.Atoi(lastSeq)
		return fmt.Sprintf("%v-%v", lastTime, lastSeqInt+1)
	}

	splitted := strings.Split(id, "-")
	time, seq := splitted[0], "0"
	if lastTime == "0" && lastSeq == "0" {
		if time == "0" {
			seq = "1"
		} else {
			seq = "0"
		}
	} else {
		if time == lastTime {
			seqInt, _ := strconv.Atoi(lastSeq)
			seq = strconv.Itoa(seqInt + 1)
		} else {
			seq = "0"
		}
	}
	return fmt.Sprintf("%v-%v", time, seq)
}
