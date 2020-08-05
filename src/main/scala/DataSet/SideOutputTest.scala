package DataSet

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @ProjectName: flink_test      
 * @Description: TODO            
 * @Author: wenjun       
 * @Date: 2020/7/30 9:41       
 **/
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })

    val processedStream: DataStream[SensorReading] = dataStream.process(new FreezingAlert())

    processedStream.print("processed data")
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    env.execute("side output test")
  }
}

//冰点报警，如果小于32F,输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if(i.temperature<32.0){
      context.output(new OutputTag[String]("freezing alert"),"freezing alert for"  + i.id)
    }
    collector.collect(i)
  }
}