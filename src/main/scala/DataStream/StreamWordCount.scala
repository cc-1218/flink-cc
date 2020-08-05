package DataStream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val hostname: String = params.get("hostname")
    val port: Int = params.getInt("port")

    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接受socket文本流
    val text: DataStream[String] = env.socketTextStream(hostname, port)

    //flatmap和map使用需要引入隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] = text.flatMap(_.split(" "))
                                                    .filter(_.nonEmpty)
                                                    .map(a => (a, 1))
                                                    .keyBy(0)
                                                    .sum(1)

    dataStream.setParallelism(1).print()

    //启动executor，执行任务
    env.execute("Socket StreamData Word Count")
  }
}
