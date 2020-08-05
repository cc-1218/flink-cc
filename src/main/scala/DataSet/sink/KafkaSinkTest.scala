package DataSet.sink

import java.util.Properties

import DataSet.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
 * @ProjectName: flink_test      
 * @Description: TODO            
 * @Author: wenjun       
 * @Date: 2020/7/29 9:46       
 **/
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //配置Kafka数据源
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    //添加source
    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("sensor", new SimpleStringSchema(), properties))

    //Transform操作
    val dataStream: DataStream[String] = inputStream
      .map(
        data => {
          val dataArray: Array[String] = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //转成String方便序列化输出
        }
      )

    //sink操作
    dataStream.addSink(new FlinkKafkaProducer010[String]("sinktest",new SimpleStringSchema(),properties))
    dataStream.print()

    //执行
    env.execute("kafka sink test")


  }
}
