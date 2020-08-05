package DataSet.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import DataSet.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink

/**
 * @ProjectName: flink_test      
 * @Description: TODO            
 * @Author: wenjun       
 * @Date: 2020/7/29 10:53       
 **/
object JdbcSinkTest {
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



    //sink
    dataStream.addSink(new MyJdbcSink())

    //执行
    env.execute("jdbc sink test")

  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //定义sql链接，预编译器
  var conn : Connection = _
  var insertStmt : PreparedStatement = _
  var updateStmt : PreparedStatement = _

  //初始化，创建链接和预编译语句
   override def open(parameters:Configuration):Unit = {
     super.open(parameters)
     conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
     insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?)")
     updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
   }

  // 调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // 如果update没有查到数据，那么执行插入语句
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }


  // 关闭时做清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}
