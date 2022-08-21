package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
)

//将日志数据写入Elasticsearch

type ESClient struct {
	client *elastic.Client  //es client
	index string // 要连接的数据库
	logDataChan chan interface{} //接受日志数据的channel
}

var(
	esClient *ESClient
)
func Init(addr,index string,maxSize,goroutineNum int) (err error) {
	client, err := elastic.NewClient(elastic.SetURL("http://"+addr))
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Println("connect to es success")
	esClient = &ESClient{
		client: client,
		index: index,
		logDataChan:  make(chan interface{},maxSize),
	}
	//从通道中取出数据
	for i := 0; i < goroutineNum; i++ {
		go sendToES()
	}
	return
}

func sendToES() {
	for  m1 := range esClient.logDataChan {
		//b,err := json.Marshal(m1)
		//if err != nil {
		//	fmt.Printf("Marshal m1 failed,err:%v\n",err)
		//	continue
		//}
		put1, err := esClient.client.Index().
			Index(esClient.index).
			BodyJson(m1).
			Do(context.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
		fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	}
}

func PutLogData(msg interface{}) {
	esClient.logDataChan <- msg
}
