// Lib for UVA analysis
import java.text.SimpleDateFormat

import org.apache.spark.{RangePartitioner, _}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//import scala.tls.nsc.transform.patmat.Logic.PropositionalLogic.True

object qm_driftDetection {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf()
    val sc = new SparkContext()
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://prdD01"), hadoopConf)

    //=============================================
    // Drift analysis
    //-------------
    // Parameters
    val ssnID = args(0)
    val acc = args(1)
    val storage_path = args(2)

    var sqlStr =
      """CREATE EXTERNAL TABLE IF NOT EXISTS user_acc.driftDetection_result
        |(ssn_id string, ch_id string, ck_id string, current_st string, pr_st string,
        | commonality string, tl string, driftStatus string, driftPoint string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t' STORED AS ORC
        |LOCATION 'storage_path/driftDetection_result'
      """.stripMargin.replace("acc", acc).replace("storage_path", storage_path)
    sqlContext.sql(sqlStr)

   /* val scope = args(1)  // singlechm, singlefr, random
    val oocPattern = args(2)  // intermittent, drift
    val check_object = args(3) //IMMC7A2100
    val currentst = args(4) // 5400-21 PILLAR DE...*/


    // Functions
    def singleToGroup = udf((commonality: String, single_obj: String) => {
      val group_eqp = commonality match {
        case "chk" => single_obj.substring(0, 9)
        case "chm" => single_obj.substring(0, 8)
        case "fr" => single_obj.substring(0, 5)
        //case "PREVIOUS" => single_obj.substring(0, 5)
        case _ => single_obj
      }
      group_eqp.toString
    })

    def checkeqp = udf( (commonality:String, tl:String, fr:String, chm:String, chk:String) => {
      val result = commonality match {
        case "chk" => chk.startsWith(tl.substring(0, 10))
        case "chm" => chm.startsWith(tl.substring(0, 9))
        case "fr" => fr.startsWith(tl.substring(0, 8))
        case _ => false
      }
      result
      /*if (commonality == "chk"){
        chk.startsWith(tl.substring(0, 10))
      } else if (commonality == "chm"){
        chm.startsWith(tl.substring(0, 9))
      } else if (commonality == "fr"){
        fr.startsWith(tl.substring(0, 8))
      } else {
        false
      }*/

    })

    def geteqp = udf( (commonality:String, fr:String, chm:String, chk:String) => {
      val result = commonality match {
        case "chk" => chk
        case "chm" => chm
        case "fr" => fr
        case _ => fr
      }
      result.toString
    })

    /*================================
    1. Read data from Hive table */
    // Assume:
    // + Data in the table is only from the current ssn id
    // + Multiple chat ID, pr st, tl
    // need to add channel_type
//    var sqlstr = """set mapreduce.input.fileinputformat.split.maxsize=128000000"""
//    sqlContext.sql(sqlstr)
    sqlStr =
      """select ssn_id, ch_id, ck_id, current_st, pr_st, commonality, tl
        |from user_acc.tl_matrix
        |where ssn_id = 'ssnID' and commonality <> 'PREVIOUS'
      """.stripMargin.replaceAll("ssnID", ssnID).replaceAll("acc", acc)

    val df1a = sqlContext.sql(sqlStr)
    df1a.cache()
    if (df1a.count == 0){
      println(ssnID + ". Data unavailable at user_acc.tl_matrix".replace("acc", acc))
      sys.exit()
    }

    sqlStr =
      """select ch_id as ch_id1, pr_st as pr_st1, fr, chm, chk, l_id, w_id
        |from user_acc.sg_tl_data
        |where ssn_id = 'ssnID' and ck_id = 0
      """.stripMargin.replace("ssnID", ssnID).replace("acc", acc)

    val df1b = sqlContext.sql(sqlStr)
    df1b.cache()
    if (df1b.count == 0){
      println(ssnID + ". Data unavailable at user_acc.sg_tl_data".replace("acc", acc))
      sys.exit()
    }
    /*var df1 = df1a.
      join(df1b, df1a("ch_id")===df1b("ch_id1") and df1a("pr_st")===df1b("pr_st1") and
        checkeqp(df1a("commonality"), df1a("tl"), df1b("fr"), df1b("chm"), df1b("chk"))).
      select(df1a("ssn_id"), df1a("ch_id"), df1a("ck_id"), df1a("current_st"), df1a("pr_st"),
        df1a("commonality"), df1a("tl"),
        df1b("l_id"), df1b("w_id"))  */

    sqlStr =
      """select ch_id as ch_id1, l_id as l_id1, w_id as w_id1, value as sp_value, sample_date
        |from user_acc.sp_clustering_result
        |where ssn_id = 'ssnID' and ck_id = 0
      """.stripMargin.replace("ssnID", ssnID).replace("acc", acc)
    val df1c = sqlContext.sql(sqlStr)
    df1c.cache()
    if (df1c.count == 0){
      println(ssnID + ". Data unavailable at user_acc.sp_clustering_result".replace("acc", acc))
      sys.exit()
    }

    val df1 = df1a.
      join(df1b, df1a("ch_id")===df1b("ch_id1") and df1a("pr_st")===df1b("pr_st1")).
      filter((df1a("commonality")==="fr" and df1b("fr").isNotNull) or
        (df1a("commonality")==="chm" and df1b("chm").isNotNull) or
        (df1a("commonality")==="chk" and df1b("chk").isNotNull)).
      filter(checkeqp(df1a("commonality"), df1a("tl"), df1b("fr"), df1b("chm"), df1b("chk"))).
      select(df1a("ssn_id"), df1a("ch_id"), df1a("ck_id"), df1a("current_st"), df1a("pr_st"),
        df1a("commonality"), df1a("tl"),
        df1b("l_id"), df1b("w_id")) //get additional cols: l,w

    val df1x = df1c.
      join(df1, df1("ch_id")===df1c("ch_id1") and df1("l_id")===df1c("l_id1") and df1("w_id")===df1c("w_id1")).
      select(df1("ssn_id"), df1("ch_id"), df1("ck_id"), df1("current_st"), df1("pr_st"),
        df1("commonality"), df1("tl"), df1("l_id"), df1("w_id"),
        df1c("sample_date"), df1c("sp_value"))

    var df2 = df1x.rdd.
      filter(r => !r.isNullAt(0) && !r.isNullAt(1) && !r.isNullAt(2) && !r.isNullAt(3) && !r.isNullAt(4)
        && !r.isNullAt(5) && !r.isNullAt(6) && !r.isNullAt(7) && !r.isNullAt(8) && !r.isNullAt(9) && !r.isNullAt(10)) // avoid null at these columns

    // Save data
    // Pls check if it can overwrite the folder, else we load old unknown data
    val filepath = "/user/acc/mikenguyen/driftDetection/data".replace("acc", acc)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true) } catch { case _ : Throwable => { } }
    df2.saveAsObjectFile(filepath)
    df2 = sc.objectFile[org.apache.spark.sql.Row](filepath)
    if (df2.count == 0){
      println(ssnID + ". Data unavailable at joint data of drift detection.")
      sys.exit()
    }

    df1a.unpersist()
    df1b.unpersist()
    df1c.unpersist()

    /*================================
    1A. Prepare data */
    //divide into tuple (context, time|value)
    //where context = "ch_id|commonality|pr_st|eqp|tl_id"

    val a = df2.map(r =>
      (r(0).toString + "|" + r(1).toString + "|" + r(2).toString + "|" + r(3).toString + "|" + r(4).toString
        + "|" + r(5).toString + "|" + r(6).toString,
      r(9).toString + "|" + r(10).toString))

    // this function put times and values into one line for piping
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    def conv(y: Iterable[String]): String = {
      var array_tup = y.map(x => (x.split("\\|")(0), x.split("\\|")(1).toDouble)).toArray  //get array of (time,value)
      array_tup = array_tup.sortBy(_._1)  //sort by time
      //extract, convert time to seconds from Epoch
      val time = array_tup.map(x => (timeFormat.parse(x._1).getTime/1000).toInt).mkString("+")
      val value = array_tup.map(x => x._2).mkString("+")  //extract value
      return time + "," + value
    }

    val ts_rdd = a.groupByKey().
      map(x => (x._1, conv(x._2)))  //put sp values together and sort by time, then convert to a string for each line of context
      //map(x => x._1 + "+" + x._2)  //combine into one string for R to read*/

    val noChannels = ts_rdd.count().toInt
    val m = ts_rdd.partitionBy(new RangePartitioner((noChannels*1.1).toInt+2, ts_rdd))

    //ts_rdd = ts_rdd.repartition(noChannels)

    val scriptPath1 = "/home/acc/qm/piped_breakout1.R".replaceAll("acc", acc) //path, not just the name
    val scriptPath2 = "/home/acc/qm/input_config.tsv".replaceAll("acc", acc) //path, not just the name
    sc.addFile(scriptPath1)
    sc.addFile(scriptPath2)
    val pipedRDD = m.pipe("./piped_breakout1.R ./input_config.tsv")
    //val result = pipedRDD.collect()
    //result.foreach(println)

    //sample code for piped R breakout
/*    import java.io.StringReader
    import au.com.bytecode.opencsv.CSVReader
    val x1 = sc.textFile("/user/hdfsf10w/mikenguyen/rda3.csv")
    val result = x1.map{line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val x2 = result.map(l => (l(0), (timeFormat.parse(l(1)).getTime/1000).toInt.toString + "|" + l(2)))
    val x3 = x2.groupByKey().map(x => (x._1, conv(x._2))).map(x => x._1 + "+" + x._2)
    val x4 = x3.repartition(2)
    //System.setProperty("spark.files.overwrite", "true")

    val scriptPath1 = "./piped_breakout1.R" //path, not just the name
    val scriptPath2 = "./input_config.tsv" //path, not just the name
    sc.addFile(scriptPath1)
    sc.addFile(scriptPath2)
    val pipeRDD = x4.pipe(scriptPath1 + " " + scriptPath2)
    val r = pipeRDD.collect
    r.foreach(println)*/

    /*================================
    4. Save output */

    val output = pipedRDD.map { l =>
      // l = "ch_id|commonality|pr_st|eqp|tl_id,driftStatus"
      val context = l.split(",")(0).split("\\|")  // context = Array(ch_id,commonality,pr_st,eqp,tl_id)
      val driftStatus = l.split(",")(1)
      val driftPoint = l.split(",")(2)
      Row(context(0), context(1), context(2), context(3), context(4), context(5), context(6),
        driftStatus, driftPoint)
    }
    val schema = StructType(Array(
      StructField("ssn_id", StringType),
      StructField("ch_id", StringType),
      StructField("ck_id", StringType),
      StructField("current_st", StringType),

      StructField("pr_st", StringType),
      StructField("commonality", StringType),
      StructField("tl", StringType),
      StructField("driftStatus", StringType),
      StructField("driftPoint", StringType)
    ))
    var result = sqlContext.createDatafr(output, schema)
    result = result.repartition(1)

    sqlContext.sql("use user_"+acc)
    result.insertInto("driftDetection_result")

    println(ssnID + ". JAR qm_driftDetection completed.")
    sys.exit()
    //===============
    //second, run for random commonality
  }
}
