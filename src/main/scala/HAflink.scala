import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, GroupedDataSet, createTypeInformation}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


object HAflink {

  def toLetterCSV(sortedGroups: GroupedDataSet[(String, Int)], char: String, directoryPath: String) = {
    sortedGroups.reduceGroup {
      (in: Iterator[(String, Int)], out: Collector[(String, Int)]) =>
        for (element <- in) {
          if (element._1 == char)
            out.collect(element)
        }

    }
      .map(f => f._2).writeAsText(directoryPath + "\\" + char, writeMode = WriteMode.OVERWRITE)
  }


  def main(args: Array[String]): Unit = {
    val source = args(0)
    val destination = args(1)


    val env = ExecutionEnvironment.getExecutionEnvironment

    val lines = env.readCsvFile[(String, Int)](source)

    val sortedGroups = lines

      .groupBy(0)
      .sortGroup(1, Order.ASCENDING)


    toLetterCSV(sortedGroups, "A", destination)
    toLetterCSV(sortedGroups, "B", destination)
    toLetterCSV(sortedGroups, "C", destination)
    toLetterCSV(sortedGroups, "D", destination)
    toLetterCSV(sortedGroups, "E", destination)
    toLetterCSV(sortedGroups, "F", destination)

    env.execute()


  }


}