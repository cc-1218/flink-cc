package DataSet

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputpath = "E:\\cwj_rx\\flink_test\\src\\main\\resources\\WordCount"

    val text: DataSet[String] = env.readTextFile(inputpath)

    val value: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map(a => (a, 1))
      .groupBy(0)
      .sum(1)

    value.print()

  }
}
