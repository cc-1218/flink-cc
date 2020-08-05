package DataSet

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.immutable
import scala.util.Random

/**
 * 对flink常见的Source数据源进行总结
 * 1.集合
 * 2.文件
 * 3.Kafka
 * 4.自定义数据源
 */
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1.从集合读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.89564231),
      SensorReading("sensor_2", 1547718201, 31.8956431),
      SensorReading("sensor_5", 1547718220, 32.89564231),
      SensorReading("sensor_7", 1547718230, 45.89564231),
      SensorReading("sensor_4", 1547718234, 15.89564231),
      SensorReading("sensor_6", 1547718330, 68.89564231)
    ))

    /*stream1.print("stream1:").setParallelism(1)
    env.execute()*/

    //2.从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("E:\\cwj_rx\\flink_test\\src\\main\\resources\\sensor.txt")

    /*stream2.print("stream2:").setParallelism(1)
    env.execute()*/

    //3.从kafka获取数据
    //3.1创建Kafka相关的配置
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    //3.2获取数据
    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("sensor", new SimpleStringSchema(), properties))

    //4.自定义数据源
    val stream4: DataStream[SensorReading] = env.addSource(new SensorSource())
    //sink输出
    stream4.print("stream4:").setParallelism(1)
    env.execute()
  }
}

//定义样例类，传感器id、时间戳、温度
case class SensorReading(id:String,timestamp:Long,temperature:Double)

class SensorSource() extends SourceFunction[SensorReading]{
  //定义一个flag：表示数据源是否正常运行
  var running:Boolean=true

  override def cancel(): Unit = running=false

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //创建一个随机数发生器
    val rand: Random = new Random()

    //随机初始化生成10个传感器的温度数据，之后在它基础随机波动生成流数据
    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    //无限循环生成流数据，除非被cancel
    while (running){
      //更新温度值
      curTemp = curTemp.map(
        t=>(t._1,t._2+rand.nextGaussian())
      )

      //获取当前时间戳
      val curTime: Long = System.currentTimeMillis()

      //包装成SensorReading,输出
      curTemp.foreach(
        t=>sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )

      //间隔100ms
      Thread.sleep(100)
    }
  }


}