import org.apache.flink.api.scala.ExecutionEnvironment
//import org.apache.flink.table.delegation


object HAflink {
  //   class Iris(SepalLength:String,
  //                  SepalWidth:Int)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lines = env.readTextFile("C:\\Users\\avdar\\projects\\flink\\homeAssignment.csv") //[(String, Int)]("C:\\Users\\avdar\\projects\\flink\\homeAssignment.csv")
    lines.print()
    env.execute()
    //    println(0)


  }


}