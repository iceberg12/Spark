// Lib for tl analysis
import org.apache.spark.mllib.stat.Statistics._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

// Lib for u analysis
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.StatCounter

//import scala.tls.nsc.transform.patmat.Logic.PropositionalLogic.True

object qm69_incomingtlu {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf()
    val sc = new SparkContext()
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://TSPROD01"), hadoopConf)

    //=================================================================================
    // tl analysis
    val ssID = args(0)  
    val acc = args(1)
    //val scope = args(1)  
    //val ocType = args(2)  
    //-------------

    def equip_oid = udf((str: String)=> try{str.toDouble} catch {
      case e: NumberFormatException => str.hashCode.toDouble%101
      case e: NullPointerException => 0
    })
    def label_double = udf((str: String) => if (str=="GOOD") {1.0} else {0.0})

    var strSql =
      """select ch_id as ch_id1, commonality, process_st as tl_st1, tl as equip_id
        |from user_acc.tl_matrix
        |where ss_id = 'ssID' and (commonality = 'PREVIOUS' or commonality like 'ddd%')
      """.stripMargin
    var strSql1 = strSql.replaceAll("ssID", ssID).replace("acc", acc)
    var df1a = sqlContext.sql(strSql1)
    df1a.cache()
    if (df1a.count == 0){
      println(ssID + ". Data unavailable at user_acc.tl_matrix".replace("acc", acc))
      sys.exit()
    }

    strSql = """SELECT distinct ch_id, process_st as tl_st, l_id, w_id, concat(l_id,'%',w_id) as lwid
          , concat(l_id,'%',w_id, '@', ch_id,'%',ckc_id ,'@',label) as id, process_st as factors
          , label, frame, chamber, chuck
          FROM user_acc.incoming_sigma_tl_data
          WHERE ss_id = 'ssID' and ckc_id = 0
          and (chamber is NULL or length(chamber) = 10)
          and (chamber is NULL or length(chamber) = 10)
          and process_st not like '0%'
          and process_st not like '1%'   """
    strSql1 = strSql.replaceAll("ssID", ssID).replace("acc", acc)
    var df1b = sqlContext.sql(strSql1)
    df1b.cache()
    if (df1b.count == 0){
      println(ssID + ". Data unavailable at user_acc.incoming_sigma_tl_data".replace("acc", acc))
      sys.exit()
    }

    def notEqualString = udf((str1: String, str2: String) => (str1 != str2))
    val df = df1a.join(df1b, df1a("ch_id1")===df1b("ch_id") and notEqualString(df1a("tl_st1"),df1b("tl_st")))
    val inputdata = df.select(df("ch_id"), df("tl_st"), df("l_id"), df("w_id"), df("lwid"), df("id"),
      df("factors"), label_double(df("label")) as "dlabel",
      equip_oid(df("chuck")) as "equip_oid1",
      equip_oid(df("chamber")) as "equip_oid2",
      equip_oid(df("frame")) as "equip_oid3",
      df("chuck") as "equip_id1",
      df("chamber") as "equip_id2",
      df("frame") as "equip_id3").rdd
    //[10,DRY ETCH,L95B,462657%0@GOOD,7909491.001,9491-11,7909491.001%9491-11,5030-20 STI FP DRY ETCH%st4GasSum (PROCESS_POSN -
    //L2KYAD1420),1.0,-66.0,199.971]
    var inputdata_repartition=inputdata.coalesce(50)

    //var filepath = "/tmp/mikenguyen/qm69/incomingu/tldata"
    var filepath = "/user/acc/mikenguyen/incomingu/inputdata_repartition".replace("acc", acc)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true) } catch { case _ : Throwable => { } }
    inputdata_repartition.saveAsObjectFile(filepath)
    inputdata_repartition = sc.objectFile[org.apache.spark.sql.Row](filepath)
    df1a.unpersist()
    df1b.unpersist()

    val columns = inputdata_repartition.map(r=>r.getString(6)).distinct.collect().sorted //get factors as columns

    //*************************************************ANALYZE FRAMES
    def analyzetl(equip:String): DataFrame = {
      var idx1 = 0
      var idx2 = 0
      equip match {
        case "FRAME" => {
          idx1 = 10
          idx2 = 13
        }
        case "CHAMBER" => {
          idx1 = 9
          idx2 = 12
        }
        case "CHUCK" => {
          idx1 = 8
          idx2 = 11
        }
        case _ => {}
      }
      val inputdataRDD = inputdata_repartition.
        keyBy(r=>r.getString(5)).  //get id as key
        aggregateByKey(Map[String, Double]())((m,r)=>
        m +(r.getString(6) -> r.getDouble(idx1)), (m_1, m_2)=> m_1 ++ m_2
        )

      val transData = inputdataRDD.map { case (key, map_factor_value) =>
        Row.fromSeq(
          (key.split("@").slice(1, 3) ++ columns.map(c => if (map_factor_value.contains(c)) map_factor_value(c).toDouble else 0.0))
            .toSeq
        )
      }
      val schema = StructType(Array(StructField("id", StringType), StructField("label", StringType))
        ++ columns.map(c => StructField(c, DoubleType)))
      val InputDataDF = sqlContext.createDataFrame(transData, schema)
      val disIDs = InputDataDF.map(r => r.getString(0)).distinct.collect()

      val ContigencyTableTest = disIDs.map { id =>
        val default = (id, Array.fill[Double](columns.length)(10))
        try {
          val oneChDF = InputDataDF.filter(InputDataDF("id") === id)
          val noObs = oneChDF.select("label").count  //data has more than 10 points?
          val vary = oneChDF.select("label").distinct.map(r => r(0).toString).collect  //the data has both G and B labels?
          val noVary = vary.length

          if (noVary < 2 | noObs < 10){  //check for valid channel ID before doing analysis
            default
          } else {
            val testRDD = oneChDF.map { r =>
              var label = 1.0
              if (r.getString(1) == vary(0)) {
                label = 0.0
              }
              var factors = Array[Double]()
              for (i <- 0 to columns.length - 1) {
                factors = factors :+ r.getDouble(i + 2)
              }
              val features = Vectors.dense(factors)
              LabeledPoint(label, features)
            }
            (id, chiSqTest(testRDD).map(_.pValue))
          }
        } catch {
          case e: Exception => default
        }
      }

      /** **method 2 *****/
      val schema2 = StructType(Array(
        StructField("id", StringType)
        , StructField("label", StringType)
        , StructField("lwid", StringType)
        , StructField("factors", StringType)
        , StructField("equip_id", StringType)
      ))
      val rddoriginal = inputdata_repartition.
        map(r => Row.fromSeq((r.getString(5).split("@").slice(1, 3)
          ++ Array(r.getString(4), r(6).toString, r.getString(idx2))).toSeq))
      val dforiginal = sqlContext.createDataFrame(rddoriginal, schema2)
      dforiginal.cache()

      val sigIDsts_2 = ContigencyTableTest.flatMap { case (ids, pArray) =>
        val ind = (0 until columns.length - 1).filter(pArray(_) < 0.05) // ***threashold 0.05
        ind.map(i => ids.toString + "@" + columns(i)) //pArray(i)
      }

      val pvalue = sc.parallelize(ContigencyTableTest.flatMap { case (ids, pArray) =>
        val ind = (0 until columns.length - 1).filter(pArray(_) < 0.05) // ***threashold 0.05
        ind.map(i => Row(ids.toString, columns(i), pArray(i))) //pArray(i) with ROW
      })
      //solve this
      val schema1 = StructType(Array(
        StructField("id", StringType),
        StructField("tl_st", StringType),
        StructField("p_value", DoubleType)
      ))
      val pvalue1 = sqlContext.createDataFrame(pvalue, schema1)
      pvalue1.count
      pvalue1.printSchema

      def keyContain = udf((str: String) => {
        sigIDsts_2.contains(str)
      })

      def concatStr = udf((str_1: String, str_2: String) => {
        str_1 + "@" + str_2
      })
      def IntToDouble = udf((c: Int) => {
        c.toDouble
      })

      // count percentage oc
      var dfsignificant = dforiginal.
        filter(keyContain(concatStr(dforiginal("id"), dforiginal("factors")))).
        filter(dforiginal("label") === "B")

      var c_equ = dfsignificant.
        groupBy("id", "factors", "equip_id").count().
        withColumnRenamed("count", "equ_count").
        withColumnRenamed("factors", "efactors").
        withColumnRenamed("id", "eid").repartition(10)
      var c_total = dfsignificant.
        groupBy("id", "factors").count().
        withColumnRenamed("count", "total_count").
        withColumnRenamed("factors", "tfactors").
        withColumnRenamed("id", "cid").repartition(10)

      var m1 = c_total.
        join(c_equ, c_equ("eid") === c_total("cid") and c_equ("efactors") === c_total("tfactors"))
      val m21 = m1.select(m1("eid"), m1("efactors"), m1("equip_id"), m1("equ_count") / m1("total_count") as "percentage")

      // count percentage of all points
      dfsignificant = dforiginal.
        filter(keyContain(concatStr(dforiginal("id"), dforiginal("factors"))))

      c_equ = dfsignificant.
        groupBy("id", "factors", "equip_id").count().
        withColumnRenamed("count", "equ_count").
        withColumnRenamed("factors", "efactors").
        withColumnRenamed("id", "eid").repartition(10)
      c_total = dfsignificant.
        groupBy("id", "factors").count().
        withColumnRenamed("count", "total_count").
        withColumnRenamed("factors", "tfactors").
        withColumnRenamed("id", "cid").repartition(10)

      m1 = c_total.
        join(c_equ, c_equ("eid") === c_total("cid") and c_equ("efactors") === c_total("tfactors"))
      val m22 = m1.select(m1("eid"), m1("efactors"), m1("equip_id"), m1("equ_count") / m1("total_count") as "percentage")

      //combine
      val m2 = m21.
        join(m22, m21("eid") === m22("eid") and m21("efactors") === m22("efactors") and m21("equip_id") === m22("equip_id")).
        select(m21("eid"), m21("efactors"), m21("equip_id"), m21("percentage") as "oc_percent", m22("percentage") as "all_percent")

      //remove null equip_id or equip_id not contributing high percentage of ocs
      def checkNullCol = udf((str:String) => str match {
        case null => false
        case _ => true
      })
      //remove ddd tls
      var percentage = m2.filter(checkNullCol(m2("equip_id")) and m2("oc_percent") > 0.8 and m2("all_percent") < 0.9)
      def firstElem = udf((str: String) => {
        str.split("%")(0)
      })
      def perToP = udf((c: Double) => {
        1 - c + 0.01
      })

      percentage = percentage.join(pvalue1,percentage("eid")===pvalue1("id")
        and percentage("efactors")===pvalue1("tl_st"))
      percentage = percentage.
        select(firstElem(percentage("eid")) as "channel_id", firstElem(percentage("efactors")) as "tl_st",
          percentage("equip_id"), percentage("p_value") as "p_value").
        withColumn("commonality", lit(equip)).
        repartition(10)

      //return
      dfsignificant.unpersist()
      return percentage
    }
    val p1 = analyzetl("FRAME")
    p1.cache()
    val p2 = analyzetl("CHAMBER")
    p2.cache()
    val p3 = analyzetl("CHUCK")
    p3.cache()

    def joinWithReplaceStr = udf((str1: String, str2:String) => str2 match {
      case null => str1
      case _ => str2
    })
    def joinWithReplaceDou = udf((d1: Double, d2:Double) => d2 match {
      case 0.0 => d1
      case _ => d2
    })
    var temp = p1.join(p2, p1("channel_id") === p2("channel_id") and p1("tl_st") === p2("tl_st")
      , "left_outer").
      select(p1("channel_id"), p1("tl_st"),
        joinWithReplaceStr(p1("equip_id"), p2("equip_id")) as "equip_id",
        joinWithReplaceDou(p1("p_value"), p2("p_value")) as "p_value",
        joinWithReplaceStr(p1("commonality"), p2("commonality")) as "commonality"
      )
    temp = temp.join(p3, temp("channel_id") === p3("channel_id") and temp("tl_st") === p3("tl_st")
      , "left_outer").
      select(temp("channel_id"), temp("tl_st"),
        joinWithReplaceStr(temp("equip_id"), p3("equip_id")) as "equip_id",
        joinWithReplaceDou(temp("p_value"), p3("p_value")) as "p_value",
        joinWithReplaceStr(temp("commonality"), p3("commonality")) as "commonality"
      )
    //add ss_id
    var tl_result = temp.withColumn("ss_id", lit(ssID)).
      select("ss_id", "channel_id", "tl_st", "commonality", "equip_id", "p_value")

    sqlContext.sql("use user_" + acc)
    val sqlStr2 =
      """CREATE TABLE IF NOT EXISTS user_acc.sp_analytics_incoming_tl_result
        |(ss_id string, ch_id string, tl_st string, commonality string, equipment string, p_value double)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t' STORED AS ORC
      """.stripMargin
    sqlContext.sql(sqlStr2.replaceAll("acc", acc))
    tl_result = tl_result.repartition(1)
    tl_result.insertInto("sp_analytics_incoming_tl_result")

    //==================================================================================================================
    // u analysis

    /*================================
    1. Read data from Hive table
    */
    // Assume:
    // + Data in the table is only from the current ss id
    // + Multiple chart ID, process st, tl
    // need to add channel_type

    def singleToGroup = udf((commonality: String, single_obj: String) => {
      val group_equip:String = commonality match {
        case "CHUCK" => single_obj.substring(0, 9)
        case "CHAMBER" => single_obj.substring(0, 8)
        case "FRAME" => single_obj.substring(0, 6)
        case _ => single_obj
      }
      group_equip
    })

    strSql =
      """select ch_id as ch_id1, tl_st as st, commonality, substring(equipment,1,10) as equip_id
        |from user_acc.sp_analytics_incoming_tl_result
        |where ss_id = 'ssID'
        |and substring(equipment,1,10) REGEXP '^[A-Za-z0-9]+$'
      """.stripMargin
    strSql1 = strSql.replaceAll("ssID", ssID).replace("acc", acc)
    df1a = sqlContext.sql(strSql1)
    df1a = df1a.select(df1a("ch_id1"), df1a("st"), df1a("commonality"), df1a("equip_id"),
      singleToGroup(df1a("commonality"), df1a("equip_id")) as "group_equip")
    df1a = df1a.repartition(20)
    df1a.cache()
    if (df1a.count == 0){
      println(ssID + ". Data unavailable at user_acc.sp_analytics_incoming_tl_result".replace("acc", acc))
      sys.exit()
    }

    //    sqlstr = """set mapreduce.input.fileinputformat.split.maxsize=32000000"""
    //    sqlContext.sql(sqlstr)
    strSql =
      """select concat(fb, '|', dg_id, '|', ss_id, '|', md, '|', ch_id, '|', label, '|', dataset, '|'
        |, tl_st, '|', tl_id, '|', l_id, '|', w_id) as c0
        |,sub_data_set as c1
        |,value
        |,ch_id, tl_st, tl_id
        |from user_acc.sp_analytics_data_u_final
        |where ss_id = 'ssID' and ckc_id = 0
      """.stripMargin
    strSql1 = strSql.replaceAll("ssID", ssID).replace("acc", acc)
    df1b = sqlContext.sql(strSql1)
    df1b = df1b.repartition(20)
    df1b.cache()
    if (df1b.count == 0){
      println(ssID + ". Data unavailable at user_acc.sp_analytics_data_u_final".replace("acc", acc))
      sys.exit()
    }

    def concat = udf((str1: String, str2: String) => {
      str1 ++ "|" + str2
    })
    /*def strStartWith = udf((tl_id: String, group_equip:String) => {
      tl_id.startsWith(group_equip)
    })*/
    val df1 = df1a.join(df1b, df1a("ch_id1")===df1b("ch_id") and df1a("st")===df1b("tl_st")
      and df1b("tl_id").startsWith(df1a("group_equip"))).
      select(concat(df1b("c0"), df1a("equip_id")) as "c0", df1b("c1"),
        df1b("value"), df1b("ch_id"), df1b("tl_st"), df1a("group_equip"), df1a("commonality"))
    //? Need to replace by df1a("equip_id")===df1b("tl_id")

    var df2 = df1.rdd.
      filter(r => !r.isNullAt(0) && !r.isNullAt(1) && !r.isNullAt(2)) // avoid null at these columns

    //filepath = "/tmp/mikenguyen/incomingu/udata"
    filepath = "/user/acc/mikenguyen/incomingu/df2".replace("acc", acc)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true) } catch { case _ : Throwable => { } }
    df2.saveAsObjectFile(filepath)
    df2 = sc.objectFile[org.apache.spark.sql.Row](filepath)
    df1a.unpersist()
    df1b.unpersist()

    //list of combinations to do u analysis on each
    val df2a = df2.filter(r => {
      val a = r(1).toString
      a.substring(a.length-3, a.length) == "ean"
    })
//    df2a.take(10).map(r => r(1).toString.split("::")(0)).foreach(println)
    val ctx = df2a.map(r => r(3).toString +"|"+ r(4).toString +"|"+ r(5).toString + "|"+ r(6).toString).distinct.collect.sorted
    df2a.cache()

    //*******************************************************
    //run u analysis for each ch_id + tl_id
    /*def IDtoLabel = udf((id: String) => {
      val label = id.split("\\|")(5)
      label match{
        case "GOOD" => 0.0
        case _ => 1.0
      }
    })*/
    def IDtoLabel = udf((id: String) => {
      var label = 0.0
      val commonality = id.split("\\|")(12)
      val badtl = id.split("\\|")(11)
      val tls = id.split("\\|")(8)
      val isBadtl: Boolean = commonality match {
        case "CHUCK" => tls.substring(0, 10) == badtl.substring(0, 10)
        case "CHAMBER" => tls.substring(0, 9) == badtl.substring(0, 9)
        case "FRAME" => tls.substring(0, 8) == badtl.substring(0, 8)
        case _ => false
      }
      if (isBadtl)
        label = 1.0
      label
    })
    def myF(str:String): Array[Row] ={
      var df2_per = df2a.filter(r => (r(3).toString +"|"+ r(4).toString +"|"+ r(5).toString +"|"+ r(6).toString) == str)

      val context = df2_per.take(1)(0)(0).toString.split("\\|")
      var method = "empty"
      var output = Array(Row(context(0), context(1), context(2), context(3), context(4), context(7), context(11)
        , "ssr_NA", 0.0, method, "l_NA", "w_NA", "st_NA", 0.0, "oc_NA"))
    try {
      val nows = df2_per.map(r => r(0).toString.split("\\|").slice(9, 11).mkString("|")).collect.distinct.length
      if (nows < 10) {
        output = Array(Row(context(0), context(1), context(2), context(3), context(4), context(7), context(11)
          , "ssr_NA", nows.toDouble, "low_num_samples", "l_NA", "w_NA", "st_NA", 0.0, "oc_NA"))
        return output
      }

      df2_per = df2_per.coalesce(100)
      df2_per.cache()

      //================================
      //2.A calculate volatility of signals per w

      // exclude extra sts due to different recipes
      val a = df2_per.map(r => Array(r(0).toString + "|" + r(6).toString,
        r(1).toString.split("::")(0) + "::" + r(1).toString.split("::")(1), r(2).toString))
      //      a.take(10).foreach(a => println(a.mkString(" ")))
      val b = a.map(x => x(1)).map(x => (x, 1)).reduceByKey(_ + _)
      val c = b.map(x => (x._2, 1)).reduceByKey(_ + _)

      val max_count = c.collect.map(x => x._2).reduceLeft(_ max _)
      val c1 = c.collect.filter(x => x._2 == max_count)(0)
      val b1 = b.filter(x => !(x._2 == c1._1)).map(x => x._1).collect
      val a1 = a.filter(x => !(b1 contains x(1)))
      a1.cache()
      var detail = a1.map(x => {
        val context = x(0).split("\\|")
        Row(context(2), context(4), context(7), context(11), context(9), context(10),
          x(1).split("::")(0), x(1).split("::")(1), x(2), context(5))
      })
      var schema = StructType(Array(
        StructField("ss_id", StringType),
        StructField("ch_id", StringType),
        StructField("tl_st", StringType),
        StructField("tl", StringType),
        StructField("l_id", StringType),
        StructField("w_id", StringType),

        StructField("ssr", StringType),
        StructField("st", StringType),
        StructField("mean", DoubleType),
        StructField("oc_flag", StringType)
      ))
      val detail1 = sqlContext.createDataFrame(detail, schema)

      // calculate volatility of signals
      def conv(y: Iterable[String]): Double = {
        var array_tup = y.
          map(x => (x.split("\\|")(0), x.split("\\|")(1).toDouble)).
          filter(x => !(x._2.isNaN)).
          toArray
        if (array_tup.length == 0) return 0
        array_tup = array_tup.sortBy(_._1)
        val value = array_tup.map(x => x._2).toList
        val firstorder: Array[Double] = new Array[Double](value.size - 1)
        (1 to firstorder.length).par foreach { i =>
          firstorder(i - 1) = value(i) - value(i - 1)
        }
        return StatCounter(firstorder).variance
      }

      val vol_rdd = a1.map(x => (x(0) + "::" + x(1).split("::")(0), x(1).split("::")(1) + "|" + x(2))).groupByKey()
      //val m = a1.filter(x => x(1).split("::")(0) == "ESChuckTempMidInnerAdjusted").map(x => x(2))
      val vol_rdd_1 = vol_rdd.map(x => (x._1, conv(x._2)))

      val df2_per1 = vol_rdd_1.map(x => Row(x._1.split("::")(0), x._1.split("::")(1), x._2))

      /*================================
      2.B Transpose the data from tall to wide format
      */
      // val keys = df2_per.map(r => r(0).toString).collect().distinct.length
      val columns = df2_per1.map(r => r(1).toString).collect().distinct.sorted // distinct signals

      val pivot = df2_per1.keyBy(row => row(0).toString).aggregateByKey(Map[String, Double]())(
        (map1, row) => map1 + (row(1).toString -> row(2).toString.toDouble),
        (map1, map2) => map1 ++ map2
      )
      //fill missing with None
      val result1 = pivot.map { case (k, map) => Row.fromSeq((Array(k) ++ columns.map(c => if (map.contains(c)) map(c) else None)).toSeq) }
      val schema1 = StructType(Array(StructField("ID", StringType)) ++ columns.map(c => StructField(c, DoubleType)))
      val fin = sqlContext.createDataFrame(result1, schema1)

      /*================================
      2.C Transform from wide format to LabeledPoint under Dense vector form
      */
      // transform wide to RDD[Labeledpoint]
      val fin1 = fin.withColumn("label", IDtoLabel(fin("ID")))
      // Map feature names to indices
      val ignored = List("ID", "label")
      val featInd = fin1.columns.diff(ignored).map(fin1.columns.indexOf(_))
      val targetInd = fin1.columns.indexOf("label")

      val unscaledData = fin1.rdd.map(r => LabeledPoint(
        r.getDouble(targetInd), // Get target value
        Vectors.dense(featInd.map(i => if (!r.isNullAt(i)) r.getDouble(i) else -1.0)) // Get double features
        //Note that volatility is already 0 to 1, so add -1.0 is reasonable
      )
      )

      /////////////////////////////////////////////////////////////////////////////////
      // Logistic Regression with L1

      val scaler = new StandardScaler(withMean = true, withStd = true).fit(unscaledData.map(x => x.features))
      val data1 = unscaledData.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
      data1.cache()

      // run logistic regression
      val model = new LogisticRegressionWithSGD()
      //      val model = new LogisticRegressionWithLBFGS()
      model.optimizer.setRegParam(0.02).setUpdater(new L1Updater)
      val lgModel = model.setIntercept(true).run(data1)
      //      val lgModel = model.setIntercept(true).setNumClasses(2).run(data1)
      val w = lgModel.weights.toArray
      val featureInd = (0 until w.length).filter(i => !w(i).equals(Double.NaN) && w(i) != 0)

      if (featureInd.length > 0) {
        var weight = featureInd.map(i => 1 / math.log(math.abs(w(i)) + 1)).toArray
        val featureName = featureInd.map(i => fin.columns(i))

        if (weight.reduceLeft(_ max _) > 1) {
          weight = weight.map(x => x / weight.reduceLeft(_ max _)/20)
        } else {
          weight = weight.map(x => x / 20)
        }
        var array_tup = (0 until weight.length).map{i =>
          (featureName(i), weight(i))
        }
        array_tup = array_tup.sortBy(_._2)
        if (array_tup.length > 5){
          array_tup = array_tup.slice(0,5)
        }
        /*================================
         4. Save output */
        /*output = (1 until weight.length).map { i =>
          Row(context(0), context(1), context(2), context(3), context(4), context(7), context(11)
            , featureName(i), weight(i), "logisticreg_l1")
        }.toArray*/
        method = "logistic_l1"
        val temp = (0 until array_tup.length).map { i =>
          Row(context(0), context(1), context(2), context(3), context(4), context(7), context(11)
            , array_tup(i)._1, array_tup(i)._2, method)
        }.toArray

        detail = sc.parallelize(temp)
        schema = StructType(Array(
          StructField("fb", StringType),
          StructField("dg_id", StringType),
          StructField("ss_id", StringType),
          StructField("md", StringType),
          StructField("ch_id", StringType),
          StructField("tl_st", StringType),
          StructField("tl", StringType),

          StructField("toogle_ssr", StringType),
          StructField("p_value", DoubleType),
          StructField("statistics_method", StringType)
        ))
        val detail2 = sqlContext.createDataFrame(detail, schema)

        val detail3 = detail2.join(detail1, detail1("ss_id") === detail2("ss_id") and detail1("ch_id") === detail2("ch_id")
          and detail1("tl_st") === detail2("tl_st") and detail1("tl") === detail2("tl")
          and detail1("ssr") === detail2("toogle_ssr")).
          select(detail2("fb"), detail2("dg_id"), detail2("ss_id"), detail2("md")
            , detail2("ch_id"), detail2("tl_st"), detail2("tl"),detail2("toogle_ssr")
            , detail2("p_value"), detail2("statistics_method")
            , detail1("l_id"), detail1("w_id"), detail1("st"), detail1("mean"), detail1("oc_flag"))
        output = detail3.rdd.collect
      }
      return output
    } catch {
      case e:Error => {
        output = Array(Row(context(0), context(1), context(2), context(3), context(4), context(7), context(11)
          , "not found", 0.0, "error", "l_NA", "w_NA", "st_NA", 0.0, "oc_NA"))
        return output
      }
      case e:Exception => {
        output = Array(Row(context(0), context(1), context(2), context(3), context(4), context(7), context(11)
          , "not found", 0.0, "error", "l_NA", "w_NA", "st_NA", 0.0, "oc_NA"))
        return output
      }
    }
    }

    val final_result = ctx.map { str => myF(str)}

    // *******************************************************
    // Output
    // *******************************************************

    val array_rows = final_result.flatten
    val final_rdd = sc.parallelize(array_rows)
    val schema = StructType(Array(
      StructField("fb", StringType),
      StructField("dg_id", StringType),
      StructField("ss_id", StringType),
      StructField("md", StringType),
      StructField("ch_id", StringType),
      StructField("tl_st", StringType),
      StructField("tl", StringType),

      StructField("toogle_ssr", StringType),
      StructField("p_value", DoubleType),
      StructField("statistics_method", StringType),

      StructField("l_id", StringType),
      StructField("w_id", StringType),
      StructField("st", StringType),
      StructField("mean", DoubleType),
      StructField("oc_flag", StringType)
    ))
    val result = sqlContext.createDataFrame(final_rdd, schema)

    val result1 = result.select("fb", "dg_id", "ss_id", "md", "ch_id", "tl_st", "tl", "toogle_ssr"
     , "p_value", "statistics_method").distinct.repartition(1)
    strSql =
      """CREATE TABLE IF NOT EXISTS user_acc.sp_analytics_incoming_u_result
        |(fb string, dg_id string, ss_id string, md string,
        |ch_id string, tl_st string, tl string, ssr string, p_value double,
        |statistics_method string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t' STORED AS ORC
      """.stripMargin
    sqlContext.sql(strSql.replaceAll("acc", acc))
    sqlContext.sql("use user_"+ acc)
    result1.insertInto("sp_analytics_incoming_u_result")

    val result2 = result.repartition(1)
    strSql =
      """CREATE EXTERNAL TABLE IF NOT EXISTS user_acc.sp_analytics_incoming_u_result_visual
        |(fb string, dg_id string, ss_id string, md string,
        |ch_id string, tl_st string, tl string, ssr string, p_value double,
        |statistics_method string, l_id string, w_id string, st string, mean string, oc_flag string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t' STORED AS ORC
      """.stripMargin
    sqlContext.sql(strSql.replace("acc", acc))
    sqlContext.sql("use user_"+acc)
    result2.insertInto("sp_analytics_incoming_u_result_visual")

    println(ssID + ". JAR qm69_incomingtlu completed.")
    sys.exit()
  }
}
