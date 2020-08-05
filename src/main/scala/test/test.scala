package test


import org.apache.flink.api.scala._

object test {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(
      "Who there?",
      "I think I hear them Stand, ho Who's there?")

    val counts = text.flatMap { _.toLowerCase.split(" ") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()
  }

}
