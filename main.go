package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"logtransfer/es"
	"logtransfer/kafka"
	"logtransfer/model"
)

//从kafka消费日志数据，写入es

func main() {
	//1、加载配置文件
	var cfg = new(model.Config)
	err := ini.MapTo(cfg,"./config/logtransfer.ini")
	if err != nil {
		fmt.Printf("load config failed,err:%v\n",err)
		panic(err)
	}
	fmt.Println("load config success")
	//2、连接es
	err = es.Init(cfg.ESConf.Address,cfg.ESConf.Index,cfg.ESConf.MaxSize,cfg.ESConf.GoroutineNum)
	if err != nil {
		fmt.Printf("connect Elasticsearch failed,err:%v\n",err)
		panic(err)
	}
	fmt.Println("connect Elasticsearch success")
	//3、连接kafka
	err = kafka.Init([]string{cfg.KafkaConf.Address},cfg.KafkaConf.Topic)
	if err != nil {
		fmt.Printf("connect kafka failed,err:%v\n",err)
		panic(err)
	}
	fmt.Println("connect kafka success")

	select {}
}
