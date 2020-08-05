package DataSet.sink

import java.util

import DataSet.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @ProjectName: flink_test      
 * @Description: TODO            
 * @Author: wenjun       
 * @Date: 2020/7/29 10:27       
 **/
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //source
    val inputStream: DataStream[String] = env.readTextFile("E:\\cwj_rx\\flink_test\\src\\main\\resources\\sensor.txt")

    //transform
    val dataStream: DataStream[SensorReading] = inputStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )

    val httphosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httphosts.add(new HttpHost("localhost",9200))


    //创建一个esSink的builder
    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](
      httphosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)

          //包装成一个Map或者JsonObject
          val json: util.HashMap[String, String] = new util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.temperature.toString)
          json.put("ts", t.timestamp.toString)

          //创建index request ,准备发送数据
          val indexRequest: IndexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          //利用index发送请求。写入数据
          requestIndexer.add(indexRequest)
          println("data saved.")
        }
      }
    )

    //sink
    dataStream.addSink(esSinkBuilder.build())

    //执行
    env.execute("es sink test")

  }
}
