package DataSet

import DataSet.sink.MyJdbcSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: flink_test      
 * @Description: TODO            
 * @Author: wenjun       
 * @Date: 2020/7/29 11:23       
 **/
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)

    //source
    //val inputStream: DataStream[String] = env.readTextFile("E:\\cwj_rx\\flink_test\\src\\main\\resources\\sensor.txt")
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)


    //transform
    val dataStream: DataStream[(String, Double)] = inputStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
    }).map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(3))
      .reduce((result, data) => (data._1, result._2.min(data._2))) //统计10秒内最低温度

    //sink
    dataStream.print()

    //执行
    env.execute("window api test")

  }
}


class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  //定义固定延迟为3秒
  val bound:Long = 3 * 1000L
  //定义当前收到的最大的时间戳
  var maxTs:Long = Long.MaxValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}


class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val bound = 1000L
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id == "sensor_1"){
      new Watermark(extractedTimestamp - bound)
    }else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}