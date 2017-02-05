/**
  * Created by zhangdenghui on 17/1/30.
  */
import org.apache.spark.{SparkConf, SparkContext,Accumulator}
import scala.collection.mutable.{ArrayBuffer, Map}

object TransE {
  var vecLen, method :Int = 0
  var rate, margin :Double = 0.0
  var relation2id, entity2id = Map[String,Int]()
  var id2entity, id2relation = Map[Int,String]()
  var left_entity, right_entity = Map[Int,Map[Int,Int]]()
  var fb_h, fb_r, fb_l = ArrayBuffer[Int]()
  var ok = Map[Pair[Int,Int],Map[Int,Int]]()
  var relation_vec = Array[Array[Double]]()
  var entity_vec = Array[Array[Double]]()
  var version :String = ""
  var entity_num, relation_num, train_num :Int = 0
  val RAND_MAX = 0x7fffffff
  val pi = 3.1415926535897932384626433832795
  var random = new util.Random

  var nepoch,nbatches,batchsize :Int =1
  var res = 0.0
  var samples = ArrayBuffer[(Int,Int,Int)]()

  var L1_flag = true

  def prepare(EntityAndId: Array[(String,Int)],
              RelationAndId: Array[(String,Int)],
              Traindata: Array[(String,String,String)]): Unit = {

    EntityAndId.foreach{
      tuple =>
        entity2id(tuple._1) = tuple._2
        id2entity(tuple._2) = tuple._1
    }
    RelationAndId.foreach{
      tuple =>
        relation2id(tuple._1) = tuple._2
        id2relation(tuple._2) = tuple._1
    }
    entity_num = EntityAndId.length
    relation_num = RelationAndId.length

    println("entity_num:"+entity_num)
    println("relation_num:"+relation_num)

    Traindata.foreach{
      re =>
        if(!left_entity.contains(relation2id(re._3)))
          left_entity(relation2id(re._3)) = Map[Int,Int]()
        if(!left_entity(relation2id(re._3)).contains(entity2id(re._1)))
          left_entity(relation2id(re._3))(entity2id(re._1)) = 0
        left_entity(relation2id(re._3))(entity2id(re._1)) += 1

        if(!right_entity.contains(relation2id(re._3)))
          right_entity(relation2id(re._3)) = Map[Int,Int]()
        if(!right_entity(relation2id(re._3)).contains(entity2id(re._2)))
          right_entity(relation2id(re._3))(entity2id(re._2)) = 0
        right_entity(relation2id(re._3))(entity2id(re._2)) += 1

        fb_h += entity2id(re._1)
        fb_r += relation2id(re._3)
        fb_l += entity2id(re._2)
        samples += ((fb_h.last,fb_l.last,fb_r.last))

        if(!ok.contains((entity2id(re._1),relation2id(re._3))))
          ok((entity2id(re._1),relation2id(re._3))) = Map[Int,Int]()
        ok((entity2id(re._1),relation2id(re._3)))(entity2id(re._2)) = 1

    }
    train_num = fb_l.length
    println("traindata num:"+train_num)

  }

  def train(vecLen:Int,rate:Double,margin:Double,method:Int,sc:SparkContext): Unit = {
    this.vecLen = vecLen
    this.rate = rate
    this.margin = margin
    this.method = method

    entity_vec = Array.ofDim[Double](entity_num,vecLen)
    relation_vec = Array.ofDim[Double](relation_num,vecLen)

    entity_vec.foreach{
      r =>
        for(i <- r.indices)
          r(i) = randn(0, 1.0/vecLen, -6/math.sqrt(vecLen), 6/math.sqrt(vecLen))
    }
    relation_vec.foreach{
      r =>
        for(i <- r.indices)
          r(i) = randn(0, 1.0/vecLen, -6/math.sqrt(vecLen), 6/math.sqrt(vecLen))

        norm(r)
    }
    sgd(sc)
  }

  def vec_len(a: Array[Double]): Double = {
    var res = 0.0
    a.foreach(x=>res+= x*x )
    res = math.sqrt(res)
    res
  }
  def norm(a: Array[Double]): Unit = {
    val x = vec_len(a)
    if(x > 1)
      for(i <- a.indices)
        a(i) = a(i)/x
  }

  def rand(min: Double, max: Double): Double = {
    min + (max-min)*util.Random.nextInt()/(RAND_MAX+1.0)
  }

  def normal(x: Double, miu: Double, sigma:Double ): Double = {
    1.0/math.sqrt(2*pi)/sigma*math.exp(-1*(x-miu)*(x-miu)/(2*sigma*sigma))
  }

  def randn(miu: Double, sigma: Double, min: Double, max: Double): Double = {
    var x,y,dScope :Double = 0.0
    do{
      x = rand(min,max)
      y = normal(x,miu,sigma)
      dScope = rand(0.0,normal(miu,miu,sigma))
    }while(dScope > y)
    x
  }

  def randMax(x:Int): Int = {
    var res = (random.nextInt()*random.nextInt())%x
    while(res<0)
      res += x
    res
  }

  def gradient(h1:Int,l1:Int,r1:Int,h2:Int,l2:Int,r2:Int,
               entityMap:Map[Int,Array[Double]],
               relationMap:Map[Int,Array[Double]]): Unit = {
    for(i <- 0 until vecLen){
      var delta = 2*(entityMap(l1)(i)-relationMap(r1)(i)-entityMap(h1)(i))
      if(L1_flag)
        if(delta > 0)
          delta = 1
        else
          delta = -1
      relationMap(r1)(i) += rate*delta
      entityMap(h1)(i) += rate*delta
      entityMap(l1)(i) -= rate*delta

      delta = 2*(entityMap(l2)(i)-relationMap(r2)(i)-entityMap(h2)(i))
      if(L1_flag)
        if(delta > 0)
          delta = 1
        else
          delta = -1
      relationMap(r2)(i) -= rate*delta
      entityMap(h2)(i) -= rate*delta
      entityMap(l2)(i) += rate*delta
    }

  }

  def train_kb(h1:Int,l1:Int,r1:Int,h2:Int,l2:Int,r2:Int,
               entityMap:Map[Int,Array[Double]],
               relationMap:Map[Int,Array[Double]],
               ac_res:Accumulator[Double]): Unit = {
    val sum1 = calc_distance(h1,l1,r1,entityMap,relationMap)
    val sum2 = calc_distance(h2,l2,r2,entityMap,relationMap)
    if(sum1 + margin > sum2){
      ac_res += margin + sum1 - sum2
      gradient(h1,l1,r1,h2,l2,r2,entityMap,relationMap)
    }
  }

  def calc_distance(h:Int, l:Int, r:Int,
                    entityMap:Map[Int,Array[Double]],
                    relationMap:Map[Int,Array[Double]]): Double = {
    var sum = 0.0
    if(L1_flag)
      for(i <- 0 until vecLen)
        sum += math.abs(entityMap(l)(i)-relationMap(r)(i)-entityMap(h)(i))
    else
      for(i <- 0 until vecLen)
        sum += math.pow(entityMap(l)(i)-relationMap(r)(i)-entityMap(h)(i),2)
    sum
  }

  def sgd(sc:SparkContext): Unit = {

    val samplesRDD = sc.parallelize(samples,4).persist()
    val bcOK = sc.broadcast(ok)
    for(epoch <- 0 until nepoch){
      res = 0
      val ac_res = sc.accumulator[Double](0.0)
      var start = System.currentTimeMillis()
      for(batch <- 0 until nbatches){
        val bcRelation_vec = sc.broadcast(relation_vec)
        val bcEntity_vec = sc.broadcast(entity_vec)

        var sampledRDD = samplesRDD
          .sample(withReplacement = true, batchsize/train_num.toDouble, System.currentTimeMillis())

        val vecRDD = sampledRDD.mapPartitions{
          iter =>
            var EntityVecMap = Map[Int,Array[Double]]()
            var RelationVecMap = Map[Int,Array[Double]]()
            var VecMap = ArrayBuffer[(Map[Int,Array[Double]],Map[Int,Array[Double]])]()

            iter.foreach{
              p=>
                if(!EntityVecMap.contains(p._1))
                  EntityVecMap(p._1) = bcEntity_vec.value(p._1)
                if(!EntityVecMap.contains(p._2))
                  EntityVecMap(p._2) = bcEntity_vec.value(p._2)
                if(!RelationVecMap.contains(p._3))
                  RelationVecMap(p._3) = bcRelation_vec.value(p._3)

                var j = randMax(entity_num)

                if(random.nextInt()%1000 < 500){
                  while(if(bcOK.value.contains((p._1,p._3)))bcOK.value((p._1,p._3)).contains(j)else false)
                    j = randMax(entity_num)
                  if(!EntityVecMap.contains(j))
                    EntityVecMap(j) = bcEntity_vec.value(j)
                  train_kb(p._1,p._2,p._3,p._1,j,p._3,EntityVecMap,RelationVecMap,ac_res)
                }
                else{
                  while(if(bcOK.value.contains((j,p._3)))bcOK.value((j,p._3)).contains(p._2)else false)
                    j = randMax(entity_num)
                  if(!EntityVecMap.contains(j))
                    EntityVecMap(j) = bcEntity_vec.value(j)
                  train_kb(p._1,p._2,p._3,j,p._2,p._3,EntityVecMap,RelationVecMap,ac_res)
                }
                norm(RelationVecMap(p._3))
                norm(EntityVecMap(p._1))
                norm(EntityVecMap(p._2))
                norm(EntityVecMap(j))
            }
            VecMap += ((EntityVecMap,RelationVecMap))
            VecMap.iterator
        }
        val vecMap = vecRDD.collect()
        vecMap.foreach{
          p=>
            p._1.foreach{
              kv=>
                entity_vec(kv._1) = kv._2
            }
            p._2.foreach{
              kv=>
                relation_vec(kv._1) = kv._2
            }
        }
      }

      var end = System.currentTimeMillis()
      println("epoch"+epoch+"  res:"+ac_res.value)
      println("epoch"+epoch+" cost time:"+(end-start)/1000.0)
    }
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
    println("prepare time:"+(end-start)/1000.0)

    var vecLen :Int = 100
    var method :Int = 1
    var rate :Double = 0.001
    var margin : Int = 1
    random.setSeed(System.currentTimeMillis())

    nepoch = 1000
    nbatches = 2
    batchsize = train_num / nbatches
    start = System.currentTimeMillis()
    train(vecLen,rate,margin,method,sc)
    end = System.currentTimeMillis()
    println("train time:"+(end-start)/1000.0)

  }
}
