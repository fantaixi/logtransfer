package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"logtransfer/es"
)

//初始化kafka连接
//从kafka里面取出日志

func Init(addr []string,topic string) (err error) {
	consumer, err := sarama.NewConsumer(addr, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	//拿到指定topic下面的所有分区列表
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		//defer pc.AsyncClose()
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				//为了将同步流程异步化，将取出的日志数据先放到channel中
				//logDataChan <- msg
				var m1 map[string]interface{}
				err = json.Unmarshal(msg.Value,&m1)
				if err != nil {
					fmt.Printf("Unmarshal msg failed,err:%v\n",err)
					continue
				}
				es.PutLogData(m1)
			}
		}(pc)
	}
	return 
}
