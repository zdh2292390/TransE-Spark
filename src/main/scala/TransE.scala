/**
  * Created by zhangdenghui on 17/1/30.
  */
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{ArrayBuffer, Map}

object TransE {
  var vecLen, method :Int = 0
  var rate, margin :Double = 0.0
  var relation2id, entity2id = Map[String,Int]()
  var id2entity, id2relation = Map[Int,String]()
  var left_entity, right_entity = Map[Int,Map[Int,Int]]()
  var fb_h, fb_r, fb_l = ArrayBuffer[Int]()
  var ok = Map[Pair[Int,Int],Map[Int,Int]]()
  var relation_vec = ArrayBuffer[Array[Double]]()
  var entity_vec = ArrayBuffer[Array[Double]]()


  def prepare(EntityAndId: Array[(String,Int)],
              RelationAndId: Array[(String,Int)],
              Traindata: Array[(String,String,String)]): Unit = {

    EntityAndId.map{
      tuple =>
        entity2id(tuple._1) = tuple._2
        id2entity(tuple._2) = tuple._1
    }
    RelationAndId.map{
      tuple =>
        relation2id(tuple._1) = tuple._2
        id2relation(tuple._2) = tuple._1
    }
    val entity_num = EntityAndId.length
    val relation_num = RelationAndId.length

    println("entity_num:"+entity_num)
    println("relation_num:"+relation_num)

    Traindata.map{
      re =>
        if(left_entity.contains(relation2id(re._3))){
          if(left_entity(relation2id(re._3)).contains(entity2id(re._1)))
            left_entity(relation2id(re._3))(entity2id(re._1)) += 1
          else
            left_entity(relation2id(re._3))(entity2id(re._1)) = 0
        }
        else
          left_entity(relation2id(re._3)) = Map[Int,Int]()

        if(right_entity.contains(relation2id(re._3))){
          if(right_entity(relation2id(re._3)).contains(entity2id(re._2)))
            right_entity(relation2id(re._3))(entity2id(re._2)) += 1
          else
            right_entity(relation2id(re._3))(entity2id(re._2)) = 0
        }
        else
          right_entity(relation2id(re._3)) = Map[Int,Int]()

        fb_h += entity2id(re._1)
        fb_r += relation2id(re._3)
        fb_l += entity2id(re._2)
        if(ok.contains((entity2id(re._1),relation2id(re._3))))
          ok((entity2id(re._1),relation2id(re._3)))(entity2id(re._2)) = 1
        else
          ok((entity2id(re._1),relation2id(re._3))) = Map[Int,Int]()
    }

    println("traindata num:"+fb_l.length)

  }

  def train(vecLen:Int,rate:Double,margin:Double,method:Int): Unit = {
    this.vecLen = vecLen
    this.rate = rate
    this.margin = margin
    this.method = method



  }

  def main(args: Array[String]): Unit = {

    var start = System.currentTimeMillis()

    val conf = new SparkConf()
      .setAppName(s"TransE-Spark")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    val f1 = sc.textFile("/Users/zhangdenghui/TransE/FB15k/entity2id.txt")
    val f2 = sc.textFile("/Users/zhangdenghui/TransE/FB15k/relation2id.txt")
    val EntityAndId = f1.map{
      line =>
      val re = line.split("\t")
      (re(0),re(1).toInt) }.collect()
    val RelationAndId = f2.map{
      line=>
      val re = line.split("\t")
      (re(0),re(1).toInt) }.collect()

    val f3 = sc.textFile("/Users/zhangdenghui/TransE/FB15k/train.txt")
    val Traindata = f3.map{
      line =>
      val re = line.split("\t")
      (re(0),re(1),re(2)) }.collect()

    prepare(EntityAndId,RelationAndId,Traindata)

    var end = System.currentTimeMillis()
    println("time:"+(end-start)/1000.0)

  }
}
