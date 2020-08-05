package DataSet.sink

import DataSet.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @ProjectName: flink_test      
 * @Description: TODO            
 * @Author: wenjun       
 * @Date: 2020/7/29 10:02       
 **/
object RedisSinkTest {
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

    //配置redis
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()


    //sink
    dataStream.addSink(new RedisSink(conf,new MyRedisMapper()))

    //执行
    env.execute("Redis sink test")

  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{
  //定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
  //把传感器id和温度值保存成哈希表  HSET key field value
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }

  //定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
  //定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}