import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, GroupedDataSet, createTypeInformation}
import org.apache.flink.util.Collector


object HAflink {

  def toLetterCSV(sortedGroups: GroupedDataSet[(String, Int)], char: String, directoryPath: String) = {
    sortedGroups.reduceGroup {
      (in:Iterator[(String,Int)], out: Collector[(String,Int)]) =>
        for (element <- in) {
          if (element._1==char)
            out.collect(element)
        }

    }.print()
  }

  //   class Iris(SepalLength:String,
  //                  SepalWidth:Int)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lines = env.readCsvFile[(String, Int)]("C:\\Users\\avdar\\projects\\flink\\homeAssignment.csv") //[(String, Int)]("C:\\Users\\avdar\\projects\\flink\\homeAssignment.csv")
    val numOfLines = lines.count()

    val sortedGroups = lines

      .groupBy(0)
      .sortGroup(1, Order.ASCENDING)


    val char = "A"
    toLetterCSV(sortedGroups,char)
    sortedGroups.reduceGroup {
      (in:Iterator[(String,Int)], out: Collector[(String,Int)]) =>
        for (element <- in) {
            if (element._1==char)
              out.collect(element)
        }

    }.print()

    //      .combineGroup{
    //        (words:Iterator[(String,Int)], out: Collector[Iterator[(String, Int)]]) =>
    //          words.foreach(println)
    //          for( let<-words)
    //            {
    ////              if (let._1=="A")
    ////                out.collect(words.foreach(println))
    //            }
    ////          words foreach (out.collect)
    //
    //
    //      }//.print()

    //        println

    sortedGroups.first(1).print()
    //      .aggregate()
    //      .first(numOfLines)
    //      .aggregate()
    //      .filter(f=>f._1=="A")
    //      .print()
    //      .writeAsCsv()
    //.first(3).print()

    env.execute()
    //    println(0)


  }


}