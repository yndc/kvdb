package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yndc/kvrpc/pb"
)

func TestSetGet(t *testing.T) {
	service := setup()
	defer clean()
	defer service.Close()

	setResponse, err := service.Set(context.Background(), &pb.SetRequest{
		Values: []*pb.KeyValue{
			{Key: []byte("one"), Value: []byte("aaa")},
			{Key: []byte("two"), Value: []byte("bbb")},
			{Key: []byte("three"), Value: []byte("ccc")},
			{Key: []byte("1"), Value: []byte("ddd")},
			{Key: []byte("2"), Value: []byte("eee")},
			{Key: []byte("3"), Value: []byte("fff")},
		},
	})

	if err != nil {
		t.Error(err)
	}

	for _, v := range setResponse.Result {
		if v != true {
			t.Fail()
		}
	}

	getResponse, err := service.Get(context.Background(), &pb.GetRequest{
		Keys: [][]byte{
			[]byte("3"),
			[]byte("2"),
			[]byte("three"),
			[]byte("one"),
			[]byte("two"),
			[]byte("1"),
		},
	})

	if eq(getResponse.Values[0].Value, []byte("fff")) == false {
		t.Fail()
	}
	if eq(getResponse.Values[1].Value, []byte("eee")) == false {
		t.Fail()
	}
	if eq(getResponse.Values[2].Value, []byte("ccc")) == false {
		t.Fail()
	}
	if eq(getResponse.Values[3].Value, []byte("aaa")) == false {
		t.Fail()
	}
	if eq(getResponse.Values[4].Value, []byte("bbb")) == false {
		t.Fail()
	}
	if eq(getResponse.Values[5].Value, []byte("ddd")) == false {
		t.Fail()
	}
}

func TestConcurrentOverlap(t *testing.T) {
	// Make an array of sequential numbers with their MD5 hash as a result
	size := 1000
	salt := "noi*()SYDdhndcMNSKLjd098u"

	original := make([][]byte, size)
	result := make([][]byte, size)
	mu := sync.Mutex{}

	sw := stopwatch()

	for i := range original {
		bytes := md5.Sum([]byte(salt + strconv.Itoa(i)))
		original[i] = bytes[:]
	}

	fmt.Printf("created sample data for %d ms\n", sw().Milliseconds())

	service := setup()
	defer clean()
	defer service.Close()

	// run the writers simultaneously (with overlapping keys too)
	writerWg := sync.WaitGroup{}
	writerSw := stopwatch()
	writerWg.Add(size)
	for i := range original {
		capturedIndex := i
		go func() {
			defer writerWg.Done()
			kvs := make([]*pb.KeyValue, 0)
			kvs = append(kvs, &pb.KeyValue{Key: []byte(strconv.Itoa(capturedIndex)), Value: original[capturedIndex]})
			if capturedIndex > 1 {
				kvs = append(kvs, &pb.KeyValue{Key: []byte(strconv.Itoa(capturedIndex - 1)), Value: original[capturedIndex-1]})
			}
			if capturedIndex < len(original)-2 {
				kvs = append(kvs, &pb.KeyValue{Key: []byte(strconv.Itoa(capturedIndex + 1)), Value: original[capturedIndex+1]})
			}
			_, err := service.Set(context.Background(), &pb.SetRequest{
				Values: kvs,
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}

	go func() {
		writerWg.Wait()
		fmt.Printf("writers done for %d ms\n", writerSw().Milliseconds())
	}()

	// run the readers at random intervals (with overlapping keys too)
	readerWg := sync.WaitGroup{}
	readerSw := stopwatch()
	readerWg.Add(size)
	for i := range original {
		capturedIndex := i
		go func() {
			hasPrev := false
			keys := make([][]byte, 0)
			if capturedIndex > 1 {
				keys = append(keys, []byte(strconv.Itoa(capturedIndex-1)))
				hasPrev = true
			}
			keys = append(keys, []byte(strconv.Itoa(capturedIndex)))
			if capturedIndex < len(original)-2 {
				keys = append(keys, []byte(strconv.Itoa(capturedIndex+1)))
			}

			finished := false
			for {
				if finished == true {
					break
				}

				res, err := service.Get(context.Background(), &pb.GetRequest{Keys: keys})
				if err != nil {
					t.Error(err)
				}
				for i, v := range res.Values {
					if v.Exists == false {
						break
					}

					mu.Lock()
					if i == 0 {
						if hasPrev {
							result[capturedIndex-1] = v.Value
						} else {
							result[capturedIndex] = v.Value
						}
					} else if i == 1 {
						if hasPrev {
							result[capturedIndex] = v.Value
						} else {
							result[capturedIndex+1] = v.Value
						}
					} else if i == 2 {
						result[capturedIndex+1] = v.Value
					}
					mu.Unlock()

					if i == len(res.Values)-1 {
						finished = true
						readerWg.Done()
					}
				}
			}
		}()
	}

	readerWg.Wait()
	fmt.Printf("readers done for %d ms\n", readerSw().Milliseconds())

	// validate the values
	for i, v := range result {
		if eq(v, original[i]) == false {
			fmt.Printf("mismatch for %d\n", i)
			t.Fail()
		}
	}
}

func TestConcurrent(t *testing.T) {
	// Make an array of sequential numbers with their MD5 hash as a result
	size := 100000
	salt := "noi*()SYDdhndcMNSKLjd098u"

	original := make([][]byte, size)
	result := make([][]byte, size)

	sw := stopwatch()

	for i := range original {
		bytes := md5.Sum([]byte(salt + strconv.Itoa(i)))
		original[i] = bytes[:]
	}

	fmt.Printf("concurrent: created sample data for %d ms\n", sw().Milliseconds())

	service := setup()
	defer clean()
	defer service.Close()

	// run the writers simultaneously (with overlapping keys)
	writerWg := sync.WaitGroup{}
	writerSw := stopwatch()
	writerWg.Add(size)
	for i := range original {
		capturedIndex := i
		go func() {
			defer writerWg.Done()
			_, err := service.Set(context.Background(), &pb.SetRequest{
				Values: []*pb.KeyValue{{Key: []byte(strconv.Itoa(capturedIndex)), Value: original[capturedIndex]}},
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}

	writerWg.Wait()
	fmt.Printf("concurrent: writers done for %d ms\n", writerSw().Milliseconds())

	// run the readers
	readerWg := sync.WaitGroup{}
	readerSw := stopwatch()
	readerWg.Add(size)
	for i := range original {
		capturedIndex := i
		go func() {
			defer readerWg.Done()
			res, err := service.Get(context.Background(), &pb.GetRequest{
				Keys: [][]byte{[]byte(strconv.Itoa(capturedIndex))},
			})
			if err != nil {
				t.Error(err)
			}
			for _, v := range res.Values {
				result[capturedIndex] = v.Value
			}
		}()
	}

	readerWg.Wait()
	fmt.Printf("concurrent: readers done for %d ms\n", readerSw().Milliseconds())

	// validate the values
	for i, v := range result {
		if eq(v, original[i]) == false {
			fmt.Printf("mismatch for %d\n", i)
			t.Fail()
		}
	}
}

func TestBatched(t *testing.T) {
	// Make an array of sequential numbers with their MD5 hash as a result
	size := 100000
	salt := "noi*()SYDdhndcMNSKLjd098u"

	original := make([][]byte, size)
	result := make([][]byte, size)

	sw := stopwatch()

	for i := range original {
		bytes := md5.Sum([]byte(salt + strconv.Itoa(i)))
		original[i] = bytes[:]
	}

	fmt.Printf("batched: created sample data for %d ms\n", sw().Milliseconds())

	service := setup()
	defer clean()
	defer service.Close()

	// run the writer
	writerSw := stopwatch()
	keyValues := make([]*pb.KeyValue, size)
	for i, v := range original {
		keyValues[i] = &pb.KeyValue{Key: []byte(strconv.Itoa(i)), Value: v}
	}
	_, err := service.Set(context.Background(), &pb.SetRequest{
		Values: keyValues,
	})
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("batched: writers done for %d ms\n", writerSw().Milliseconds())

	// run the reader
	readerSw := stopwatch()
	keys := make([][]byte, size)
	for i := range original {
		keys[i] = []byte(strconv.Itoa(i))
	}
	res, err := service.Get(context.Background(), &pb.GetRequest{
		Keys: keys,
	})
	if err != nil {
		t.Error(err)
	}
	for i, v := range res.Values {
		result[i] = v.Value
	}

	fmt.Printf("batched: readers done for %d ms\n", readerSw().Milliseconds())

	// validate the values
	for i, v := range result {
		if eq(v, original[i]) == false {
			fmt.Printf("mismatch for %d\n", i)
			t.Fail()
		}
	}
}

func setup() *Service {
	clean()
	config := &config{
		port:     3000,
		path:     "./test_db",
		loglevel: "error",
	}

	return NewService(config)
}

func clean() {
	os.RemoveAll("./test_db")
}

func eq(one []byte, two []byte) bool {
	if len(one) != len(two) {
		return false
	}
	for i, v := range one {
		if v != two[i] {
			return false
		}
	}
	return true
}

func stopwatch() func() time.Duration {
	start := time.Now()
	return func() time.Duration {
		elapsed := time.Since(start)
		start = time.Now()
		return elapsed
	}
}
