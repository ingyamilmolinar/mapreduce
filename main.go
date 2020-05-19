package main

import (
	"fmt"
	"strings"

	"github.com/yamil-rivera/mapreduce/mapreduce"
)

type wordSplitter struct{}

func (this wordSplitter) Map(filename, contents string) []mapreduce.Pair {
	var list []mapreduce.Pair
	sentences := strings.Split(contents, string('\n'))
	for _, sentence := range sentences {
		words := strings.Split(sentence, " ")
		if len(words) == 0 || (len(words) == 1 && words[0] == "") {
			continue
		}
		for _, word := range words {
			list = append(list, mapreduce.Pair{word, 1})
		}
	}
	return list
}

type wordCounter struct{}

func (this wordCounter) Reduce(word string, countList []uint64) uint64 {
	var result uint64
	for _, count := range countList {
		result += count
	}
	return result
}

func main() {
	inputs := []string{
		"in1",
		"in2",
		"in3",
		"in4",
	}
	result, err := mapreduce.ScheduleJob(inputs, wordSplitter{}, wordCounter{})
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
