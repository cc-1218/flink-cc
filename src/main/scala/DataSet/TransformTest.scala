package DataSet

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: flink_test      
 * @Description:  测试flink常见的转换操作
 * @Author: wenjun       
 * @Date: 2020/7/28
 *
 **/
object TransformTest {
  def main(args: Array[String]): Unit = {
    //1.获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读入数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\cwj_rx\\flink_test\\src\\main\\resources\\sensor.txt")

    //transform操作
    val dataStream: DataStream[SensorReading] = inputStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )

    //聚合操作
    val stream1: DataStream[SensorReading] = dataStream.keyBy("id")
      //.sum("temperature")
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
   /* stream1.print("stream1:").setParallelism(1)
    env.execute("transform test job")*/

    //分流，根据温度是否大于30度划分
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val highDataStream: DataStream[SensorReading] = splitStream.select("high")
    val lowDataStreame: DataStream[SensorReading] = splitStream.select("low")
    val allDataStream: DataStream[SensorReading] = splitStream.select("high", "low")

    /*highDataStream.print("highDataStream:").setParallelism(1)
    lowDataStreame.print("lowDataStreame:").setParallelism(1)
    allDataStream.print("allDataStream:").setParallelism(1)*/

    //合并两条流
    val warningStream: DataStream[(String, Double)] = highDataStream.map(sensorDate => (sensorDate.id, sensorDate.temperature))
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowDataStreame)

    val coMapStream: DataStream[Product with Serializable] = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temperature warning"),
      lowData => (lowData.id, "healthy")
    )

    //coMapStream.print("coMapStream:").setParallelism(1)

    val unionStream: DataStream[SensorReading] = highDataStream.union(lowDataStreame)
    //unionStream.print("unionStream:").setParallelism(1)


    //函数类
    dataStream.filter(new MyFilter()).print()





    env.execute("transform test job")
  }
}


class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}