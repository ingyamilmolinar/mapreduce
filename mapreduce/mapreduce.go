package mapreduce

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io/ioutil"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type Pair struct {
	K string
	V uint64
}

type Mapper interface {
	Map(filename, contents string) []Pair
}

type Reducer interface {
	Reduce(word string, countList []uint64) uint64
}

type master struct{}

func (this *master) scheduleMappers(files []string, mapper Mapper) {
	var wg sync.WaitGroup
	for _, file := range files {
		var worker worker
		wg.Add(1)
		go worker.ExecuteMapper(file, mapper, &wg)
	}
	wg.Wait()
}

func (this *master) scheduleReducers(files []string, reducer Reducer) {
	var wg sync.WaitGroup
	for _, file := range files {
		var worker worker
		wg.Add(1)
		go worker.ExecuteReducer("inter_"+file, "out_"+file, reducer, &wg)
	}
	wg.Wait()
}

type worker struct{}

func (this *worker) ExecuteMapper(file string, mapper Mapper, wg *sync.WaitGroup) error {
	defer wg.Done()
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	pairList := mapper.Map(file, string(bytes))
	bytes, err = encodePairs(pairList)
	if err != nil {
		return err
	}
	return ioutil.WriteFile("inter_"+file, bytes, 0644)
}

func (this *worker) ExecuteReducer(inFile string, outFile string, reducer Reducer, wg *sync.WaitGroup) error {
	defer wg.Done()
	bytes, err := ioutil.ReadFile(inFile)
	if err != nil {
		return err
	}

	pairList, err := decodePairs(bytes)
	if err != nil {
		return err
	}

	sort.Slice(pairList, func(i, j int) bool {
		return pairList[i].K <= pairList[j].K
	})

	f, err := os.OpenFile(outFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	prevK := pairList[0].K
	var valueList []uint64
	for _, pair := range pairList {
		if prevK != pair.K {
			result := reducer.Reduce(prevK, valueList)
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, result)
			if _, err := f.Write(b); err != nil {
				return err
			}
			prevK = pair.K
			valueList = []uint64{}
		}
		valueList = append(valueList, pair.V)
	}
	result := reducer.Reduce(prevK, valueList)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, result)
	_, err = f.Write(b)
	return err
}

func ScheduleJob(files []string, mapper Mapper, reducer Reducer) (uint64, error) {
	var master master
	for _, file := range files {
		os.Remove("inter_" + file)
		os.Remove("out_" + file)
	}
	master.scheduleMappers(files, mapper)
	master.scheduleReducers(files, reducer)

	total := uint64(0)
	for _, file := range files {
		bytes, err := ioutil.ReadFile("out_" + file)
		if err != nil {
			return 0, err
		}
		for i := 0; i < len(bytes); i += 8 {
			total += binary.LittleEndian.Uint64(bytes[i : i+8])
		}
	}
	return total, nil
}

func decodePairs(buf []byte) ([]Pair, error) {
	var target []Pair
	dec := gob.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&target); err != nil {
		return nil, errors.Wrap(err, "Error trying to decode pairs")
	}
	return target, nil
}

func encodePairs(source []Pair) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(source); err != nil {
		return nil, errors.Wrap(err, "Error trying to encode pairs")
	}
	return buf.Bytes(), nil
}
