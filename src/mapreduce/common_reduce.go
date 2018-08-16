package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
	"log"
)

type ByKey []KeyValue
func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	files := make([]*os.File, nMap);
	decoders := make([]*json.Decoder, nMap);
	var entries [] KeyValue;
	for i:=0; i< nMap; i++ {
		files[i],_ = os.Open(reduceName(jobName,i,reduceTask));
		decoders[i] = json.NewDecoder(files[i]);
	}
	for i := 0; i < nMap; i++ {
		var err error;
		for err == nil {
			var kv KeyValue;
			err = decoders[i].Decode(&kv);
			entries = append(entries, kv);
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key;
	})
	m := make(map[string] []string);
	for _, entry := range(entries) {
		if entry.Key == "" {
			continue;
		}
		m[entry.Key] = append(m[entry.Key], entry.Value);
	}
	outputFile,err := os.Create(outFile);
	if err != nil {
		log.Fatal("Create File: ", err)
	}
	enc := json.NewEncoder(outputFile);
	for _,entry := range entries {
		key := entry.Key
		vs,ok := m[key];
		if ok {
			enc.Encode(KeyValue{key, reduceF(key, vs)});
			delete(m, key);
		}
	}
	outputFile.Close();
	for _, file := range files {
		file.Close();
	}
}
