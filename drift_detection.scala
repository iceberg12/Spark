// Lib for UVA analysis
import java.text.SimpleDateFormat

import org.apache.spark.{RangePartitioner, _}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//import scala.tools.nsc.transform.patmat.Logic.PropositionalLogic.True

object qm69_driftDetection {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf()
    val sc = new SparkContext()
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://TSPROD01"), hadoopConf)

    //=============================================
    // Drift analysis
    //-------------
    // Parameters
    val sessionID = args(0)  // val sessionID = "20160331-0700"  20160408-0800
    val acc = args(1)
    val storage_path = args(2)

    var sqlStr =
      """CREATE EXTERNAL TABLE IF NOT EXISTS user_acc.driftDetection_result
        |(session_id string, ch_id string, ckc_id string, current_step string, process_step string,
        | commonality string, tool string, driftStatus string, driftPoint string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t' STORED AS ORC
        |LOCATION 'storage_path/driftDetection_result'
      """.stripMargin.replace("acc", acc).replace("storage_path", storage_path)
    sqlContext.sql(sqlStr)

   /* val scope = args(1)  // singlechamber, singleframe, random
    val oocPattern = args(2)  // intermittent, drift
    val check_object = args(3) //IMMC7A2100
    val currentStep = args(4) // 5400-21 PILLAR DE...*/


    // Functions
    def singleToGroup = udf((commonality: String, single_obj: String) => {
      val group_equip = commonality match {
        case "CHUCK" => single_obj.substring(0, 9)
        case "CHAMBER" => single_obj.substring(0, 8)
        case "FRAME" => single_obj.substring(0, 5)
        //case "PREVIOUS" => single_obj.substring(0, 5)
        case _ => single_obj
      }
      group_equip.toString
    })

    def checkEquip = udf( (commonality:String, tool:String, frame:String, chamber:String, chuck:String) => {
      val result = commonality match {
        case "CHUCK" => chuck.startsWith(tool.substring(0, 10))
        case "CHAMBER" => chamber.startsWith(tool.substring(0, 9))
        case "FRAME" => frame.startsWith(tool.substring(0, 8))
        case _ => false
      }
      result
      /*if (commonality == "CHUCK"){
        chuck.startsWith(tool.substring(0, 10))
      } else if (commonality == "CHAMBER"){
        chamber.startsWith(tool.substring(0, 9))
      } else if (commonality == "FRAME"){
        frame.startsWith(tool.substring(0, 8))
      } else {
        false
      }*/

    })

    def getEquip = udf( (commonality:String, frame:String, chamber:String, chuck:String) => {
      val result = commonality match {
        case "CHUCK" => chuck
        case "CHAMBER" => chamber
        case "FRAME" => frame
        case _ => frame
      }
      result.toString
    })

    /*================================
    1. Read data from Hive table */
    // Assume:
    // + Data in the table is only from the current session id
    // + Multiple chart ID, process step, tool
    // need to add channel_type
//    var sqlstr = """set mapreduce.input.fileinputformat.split.maxsize=128000000"""
//    sqlContext.sql(sqlstr)
    sqlStr =
      """select session_id, ch_id, ckc_id, current_step, process_step, commonality, tool
        |from user_acc.tool_matrix
        |where session_id = 'sessionID' and commonality <> 'PREVIOUS'
      """.stripMargin.replaceAll("sessionID", sessionID).replaceAll("acc", acc)

    val df1a = sqlContext.sql(sqlStr)
    df1a.cache()
    if (df1a.count == 0){
      println(sessionID + ". Data unavailable at user_acc.tool_matrix".replace("acc", acc))
      sys.exit()
    }

    sqlStr =
      """select ch_id as ch_id1, process_step as process_step1, frame, chamber, chuck, lot_id, wafer_id
        |from user_acc.sigma_tool_data
        |where session_id = 'sessionID' and ckc_id = 0
      """.stripMargin.replace("sessionID", sessionID).replace("acc", acc)

    val df1b = sqlContext.sql(sqlStr)
    df1b.cache()
    if (df1b.count == 0){
      println(sessionID + ". Data unavailable at user_acc.sigma_tool_data".replace("acc", acc))
      sys.exit()
    }
    /*var df1 = df1a.
      join(df1b, df1a("ch_id")===df1b("ch_id1") and df1a("process_step")===df1b("process_step1") and
        checkEquip(df1a("commonality"), df1a("tool"), df1b("frame"), df1b("chamber"), df1b("chuck"))).
      select(df1a("session_id"), df1a("ch_id"), df1a("ckc_id"), df1a("current_step"), df1a("process_step"),
        df1a("commonality"), df1a("tool"),
        df1b("lot_id"), df1b("wafer_id"))  */

    sqlStr =
      """select ch_id as ch_id1, lot_id as lot_id1, wafer_id as wafer_id1, value as SPC_value, sample_date
        |from user_acc.space_clustering_result
        |where session_id = 'sessionID' and ckc_id = 0
      """.stripMargin.replace("sessionID", sessionID).replace("acc", acc)
    val df1c = sqlContext.sql(sqlStr)
    df1c.cache()
    if (df1c.count == 0){
      println(sessionID + ". Data unavailable at user_acc.space_clustering_result".replace("acc", acc))
      sys.exit()
    }

    val df1 = df1a.
      join(df1b, df1a("ch_id")===df1b("ch_id1") and df1a("process_step")===df1b("process_step1")).
      filter((df1a("commonality")==="FRAME" and df1b("frame").isNotNull) or
        (df1a("commonality")==="CHAMBER" and df1b("chamber").isNotNull) or
        (df1a("commonality")==="CHUCK" and df1b("chuck").isNotNull)).
      filter(checkEquip(df1a("commonality"), df1a("tool"), df1b("frame"), df1b("chamber"), df1b("chuck"))).
      select(df1a("session_id"), df1a("ch_id"), df1a("ckc_id"), df1a("current_step"), df1a("process_step"),
        df1a("commonality"), df1a("tool"),
        df1b("lot_id"), df1b("wafer_id")) //get additional cols: lot,wafer

    val df1x = df1c.
      join(df1, df1("ch_id")===df1c("ch_id1") and df1("lot_id")===df1c("lot_id1") and df1("wafer_id")===df1c("wafer_id1")).
      select(df1("session_id"), df1("ch_id"), df1("ckc_id"), df1("current_step"), df1("process_step"),
        df1("commonality"), df1("tool"), df1("lot_id"), df1("wafer_id"),
        df1c("sample_date"), df1c("SPC_value"))

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
      println(sessionID + ". Data unavailable at joint data of drift detection.")
      sys.exit()
    }

    df1a.unpersist()
    df1b.unpersist()
    df1c.unpersist()

    /*================================
    1A. Prepare data */
    //divide into tuple (context, time|value)
    //where context = "ch_id|commonality|process_step|equip|tool_id"

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
      map(x => (x._1, conv(x._2)))  //put SPC values together and sort by time, then convert to a string for each line of context
      //map(x => x._1 + "+" + x._2)  //combine into one string for R to read*/

    val noChannels = ts_rdd.count().toInt
    val m = ts_rdd.partitionBy(new RangePartitioner((noChannels*1.1).toInt+2, ts_rdd))

    //ts_rdd = ts_rdd.repartition(noChannels)

    val scriptPath1 = "/home/acc/qm69/piped_breakout1.R".replaceAll("acc", acc) //path, not just the name
    val scriptPath2 = "/home/acc/qm69/input_config.tsv".replaceAll("acc", acc) //path, not just the name
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
      // l = "ch_id|commonality|process_step|equip|tool_id,driftStatus"
      val context = l.split(",")(0).split("\\|")  // context = Array(ch_id,commonality,process_step,equip,tool_id)
      val driftStatus = l.split(",")(1)
      val driftPoint = l.split(",")(2)
      Row(context(0), context(1), context(2), context(3), context(4), context(5), context(6),
        driftStatus, driftPoint)
    }
    val schema = StructType(Array(
      StructField("session_id", StringType),
      StructField("ch_id", StringType),
      StructField("ckc_id", StringType),
      StructField("current_step", StringType),

      StructField("process_step", StringType),
      StructField("commonality", StringType),
      StructField("tool", StringType),
      StructField("driftStatus", StringType),
      StructField("driftPoint", StringType)
    ))
    var result = sqlContext.createDataFrame(output, schema)
    result = result.repartition(1)

    sqlContext.sql("use user_"+acc)
    result.insertInto("driftDetection_result")

    println(sessionID + ". JAR qm69_driftDetection completed.")
    sys.exit()
    //===============
    //second, run for random commonality
  }
}
