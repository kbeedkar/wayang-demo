import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.basic.data.{Record, Tuple2}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.postgres.operators.PostgresTableSource
import org.apache.wayang.spark.Spark


object TpchExample {
  def main(args: Array[String]): Unit = {

    val configuration = new Configuration()
    configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/tpch")
    configuration.setProperty("wayang.postgres.jdbc.user", "postgres")

    val wayangContext = new WayangContext(configuration)
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin())
      .withPlugin(Postgres.plugin())

     val planBuilder = new PlanBuilder(wayangContext)
      .withJobName("Tpch-query")
      .withUdfJarsOf(this.getClass)

    // Read the input sources
    // Read jdbc source (orders[orderkey, totalprice]
    val orders = planBuilder.readTable(new PostgresTableSource("orders"))
      .map { record =>
        (record.getInt(0), record.getDouble(3))
      }

    // Read file source (lineitem[orderkey, quantity]

    val lineitem = planBuilder.readTextFile("file:/tmp/lineitem.tbl")
      .map { line => {
        val cols: Array[String] = line.split("\\|")
        (cols(0).toInt, cols(4).toDouble)
      }}


    // Perform orders join (orderkey) lineitem
    val output = orders.join[(Int, Double),Int](_._1, lineitem, _._1)
      .collect()

    printRecords(output)
  }

  def printRecords(output: Iterable[Tuple2[(Int, Double), (Int, Double)]]): Unit = {
    for (record <- output) {
      println(
        record.field0._1 + " | " + record.field0._2 + " | " +
          record.field1._1 + " | " + record.field1._2
      )
    }
  }
}
