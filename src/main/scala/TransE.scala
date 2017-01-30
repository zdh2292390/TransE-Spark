/**
  * Created by zhangdenghui on 17/1/30.
  */
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map
object TransE {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"TransE-Spark")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    val f1 = sc.textFile("/Users/zhangdenghui/TransE/FB15k/entity2id.txt")
    val f2 = sc.textFile("/Users/zhangdenghui/TransE/FB15k/relation2id.txt")

    var relation2id = Map[String,Int]()
    var entity2id = Map[String,Int]()
    var id2entity = Map[Int,String]()
    var id2relation = Map[Int,String]()

    val EntityAndId = f1.map{line=>
      val re=line.split("\t")
      (re(0),re(1).toInt) }.collect()

    val RelationAndId = f2.map{line=>
      val re=line.split("\t")
      (re(0),re(1).toInt) }.collect()

    EntityAndId.map{tuple => entity2id(tuple._1) = tuple._2
      id2entity(tuple._2)=tuple._1
    }
    RelationAndId.map{tuple => relation2id(tuple._1) = tuple._2
      id2relation(tuple._2)=tuple._1
    }






  }
}
