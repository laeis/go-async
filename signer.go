package main

import (
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	buffSize = 100
)

// сюда писать код
func getCrc32Hash(pos int, data string, buff []string, wg *sync.WaitGroup) {
	defer wg.Done()
	buff[pos] = DataSignerCrc32(data)
}

//just for test get async data, this code can be in SingleHash function
func getSignerMd5(data string, mu *sync.Mutex) chan string {
	md5Chan := make(chan string, 1)
	go func() {
		mu.Lock()
		md5Chan <- DataSignerMd5(data)
		mu.Unlock()
	}()
	return md5Chan
}
// the same as function before
func calcSingleHash(stringData string, md5 string) chan interface{} {
	singleHash := make(chan interface{})
	go func(tringData string, md5 string) {
		partOfString := make([]string, 2)
		wg1 := &sync.WaitGroup{}
		wg1.Add(1)
		go getCrc32Hash(0, stringData, partOfString, wg1)
		wg1.Add(1)
		go getCrc32Hash(1, md5, partOfString, wg1)
		wg1.Wait()
		singleHash <- strings.Join(partOfString, "~")
	}(stringData, md5)
	return singleHash
}

//SingleHash make multihash string from incoming data
func SingleHash(in, out chan interface{}) {

	// DataSignerCrc32(data) + "~" + DataSignerCrc32(md5)
	wgMaine := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	for data := range in {
		wgMaine.Add(1)
		go func(data interface{}, wgMaine *sync.WaitGroup, mu *sync.Mutex) {
			defer wgMaine.Done()

			stringData := strconv.FormatInt(int64(data.(int)), 10)	
			md5Chan := getSignerMd5(stringData, mu)
			md5 := <-md5Chan
			
			calcHashChannel := calcSingleHash(stringData, md5)
			resultString := <-calcHashChannel
			//set result in next function
			out <- resultString
		}(data, wgMaine, mu)
	}
	wgMaine.Wait()
}

//MultiHash make multihash string from incoming data
func MultiHash(in, out chan interface{}) {
	wgMaine := &sync.WaitGroup{}
	for data := range in {
		println(data.(string))
		wgMaine.Add(1)
		go func(data interface{}, wgMaine *sync.WaitGroup) {
			defer wgMaine.Done()

			wg := &sync.WaitGroup{}
			partOfString := make([]string, 6)
			for th := 0; th < 6; th++ {
				wg.Add(1)
				stringForHash := strconv.FormatInt(int64(th), 10) + data.(string)
				go getCrc32Hash(th, stringForHash, partOfString, wg)
			}
			wg.Wait()
			out <- strings.Join(partOfString, "")
		}(data, wgMaine)

	}
	wgMaine.Wait()
}

//CombineResults combine result from "in" channel in a sorted string with delimiter "_"
func CombineResults(in, out chan interface{}) {
	var resultString []string
	for data := range in {
		println(data.(string))
		resultString = append(resultString, data.(string))
	}
	sort.Strings(resultString)

	out <- strings.Join(resultString, "_")
}

func afterPipeElementsFinish(wg *sync.WaitGroup, pos int, out chan interface{}) {
	wg.Done()
	close(out)
}

// ExecutePipeline this function for export and run in main maine function
func ExecutePipeline(params ...job) {
	//make necessaru for function chanels
	in := make(chan interface{}, buffSize)
	out := make(chan interface{}, buffSize)
	wg := &sync.WaitGroup{}
	wg.Add(len(params))
	for i, calback := range params {
		//create new channel what will be relate each next function
		// close everyone new channel in defer
		if i != 0 {
			out = make(chan interface{}, 100)
		}
		//maine gorutine for pipeline queue
		go func(i int, calback job, in, out chan interface{}, wg *sync.WaitGroup) {
			defer afterPipeElementsFinish(wg, i, out)
			calback(in, out)
			runtime.Gosched()
		}(i, calback, in, out, wg)
		//change chanel for next iteeration
		in, out = out, in
	}
	wg.Wait()
}
