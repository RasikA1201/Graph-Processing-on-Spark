//Name:Rasika Hedaoo
//Student ID:1001770527

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  val depth = 6

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("GraphSc");
    conf.setMaster("local[2]")
    val spConf = new SparkContext(conf);

    var count = 0
    var graph = spConf.textFile(args(0)).map(line => {val node = line.split(",");
      count += 1; (node(0).toLong, if (count <= 5) node(0).toLong else -1,
        node.map(_.toLong).toList.slice(1,node.size))})
    for (i<- 1 to depth){
      graph = graph.flatMap{a => (a._1,a._2); a._3.map{b => (b, if (a._2 > -1) a._2 else -1)}}
        .reduceByKey(_ max _)
        .join(graph.map(b => (b._1,(b._2,b._3))))
        .map{b => (b._1, if (b._2._2._1 > -1) b._2._2._1 else b._2._1, b._2._2._2)}
    }
    val graphOut = graph.map(b => (b._2, 1)).reduceByKey(_+_).sortByKey()
    graphOut.collect().foreach(println)
  }
}






