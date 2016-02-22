/**
 * Created by mikenguyen on 1/29/2016.
 */

import org.apache.spark.{sql, SparkConf, SparkContext}
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkContext._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult

import org.apache.spark.rdd

import scala.math._
import java.util.Calendar
import java.text.SimpleDateFormat


object Main {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf()
    val sc = new SparkContext()
    //    UserGroupInformation.setConfiguration();
    //==================================================================================================================
    // WIS OOC
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // Data query
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = Calendar.getInstance
    //now.add(Calendar.DAY_OF_YEAR, -7)
    val today = timeFormat.format(now.getTime)
    val m_0 = now.get(Calendar.MONTH) + 1
    val y_0 = now.get(Calendar.YEAR)

    now.add(Calendar.DAY_OF_YEAR, -7)
    val last7day = timeFormat.format(now.getTime)
    val m_70 = now.get(Calendar.MONTH) + 1
    val y_70 = now.get(Calendar.YEAR)

    now.add(Calendar.DAY_OF_YEAR, -14)
    val last21day = timeFormat.format(now.getTime)
    val m_1 = now.get(Calendar.MONTH) + 1
    val y_1 = now.get(Calendar.YEAR)

    now.add(Calendar.MONTH, -1)
    val m_2 = now.get(Calendar.MONTH) + 1
    val y_2 = now.get(Calendar.YEAR)

    def wisToDouble = udf((str: String) => str match {
      case null => 0.0
      case _ => 1.0
    })

    def wisToDouble1 = udf((str: String) => str match {
      case "null" => 0.0
      case _ => 1.0
    })

    def getConcatenated2 = udf((s1: String, s2: String) => {
      s1 + "|" + s2
    })
    def getConcatenated3 = udf((s1: String, s2: String, s3: String) => {
      s1 + "|" + s2 + "|" + s3
    })
    def strToIntstr = udf((str: String) => {
      (str.map(c => (if (c.isDigit) c.toString else (c.toInt % 100).toString))
        .mkString.toDouble % 1009).toString
      // "abc".getBytes.mkString.toDouble
    })

    val sqlStr = """SELECT DISTINCT SV.wis_ooc, cast(SC.ch_id as String) ch_id, SLW.exval_01 PartType, SLW.exval_06 as process_step_name, SLW.parameter_name
            , cast(SLW.sample_id as String) sample_id, SLW.exval_12 as lot_id, regexp_replace(SLW.exval_12, '\\.0\\p{Alnum}', '\\.00') as parent_lot
            , SLW.sample_date, SR.raw_value
            , FLH.step_name, FLEH.equip_id
      FROM (SELECT sample_id, parameter_name, sample_date, exval_01, exval_06, exval_12, month FROM PROD_MTI_SINGAPORE_FAB_7_SPACE.IDL_SAMPLES_WAFER a
            WHERE (month = 'm_0' or month = 'm_70')
            AND sample_date > cast('last7day' as timestamp)
            UNION ALL
            SELECT sample_id, parameter_name, sample_date, exval_01, exval_06, exval_12, month FROM PROD_MTI_SINGAPORE_FAB_7_SPACE.IDL_SAMPLES_LOT b
            WHERE (month = 'm_0' or month = 'm_70')
            AND sample_date > cast('last7day' as timestamp)
            ) SLW
      INNER JOIN (SELECT sample_id, raw_value, month FROM PROD_MTI_SINGAPORE_FAB_7_SPACE.IDL_SAMPLES_RAW_WAFER
            WHERE (month = 'm_0' or month = 'm_70')
            UNION ALL
            SELECT sample_id, max(raw_value) as raw_value, month FROM PROD_MTI_SINGAPORE_FAB_7_SPACE.IDL_SAMPLES_RAW_LOT
            WHERE (month = 'm_0' or month = 'm_70')
            GROUP BY sample_id, month
            ) SR
        ON SR.sample_id = SLW.sample_id
      INNER JOIN (SELECT ch_id, sample_id, out_of_order, ckc_id, month FROM PROD_MTI_SINGAPORE_FAB_7_SPACE.IDL_SAMPLES_CALC
            WHERE month = 'm_0' or month = 'm_70'
            AND out_of_order = 'N'
            AND ckc_id = 0) SC
        ON SC.sample_id = SLW.sample_id
      INNER JOIN (SELECT * FROM PROD_MTI_SINGAPORE_FAB_7_SPACE.IDL_T_CHANNEL_DEF
                  WHERE cf_value_11 IN ('PHOTO')) TCD
        ON SC.ch_id = TCD.ch_id
      LEFT JOIN (SELECT DISTINCT sample_date,ch_id as sv_ch_id, sample_id,'1' AS wis_ooc,month
            FROM PROD_MTI_SINGAPORE_FAB_7_SPACE.IDL_SAMPLES_VIOL) SV
        ON (SLW.sample_id = SV.sample_id and SV.sv_ch_id = SC.ch_id)
      INNER JOIN PROD_MTI_SINGAPORE_FAB_7_FABTRKG.IDL_FAB_LOT_HISTORY FLH
        ON ((FLH.month='m_0' and FLH.year = 'y_0') or (FLH.month='m_1' and FLH.year = 'y_1') or (FLH.month='m_2' AND FLH.year = 'y_2'))
        AND FLH.lot_id = regexp_replace(SLW.exval_12, '\\.0\\p{Alnum}', '\\.00')
        AND FLH.tracked_out_datetime > date_sub(SLW.sample_date, 4)
        AND FLH.tracked_out_datetime < SLW.sample_date
        AND FLH.step_name not like '1%'
      INNER JOIN PROD_MTI_SINGAPORE_FAB_7_FABTRKG.IDL_FAB_LOT_EQUIP_HISTORY FLEH
        ON ((FLEH.month='m_0' and FLEH.year = 'y_0') or (FLEH.month='m_1' and FLEH.year = 'y_1') or (FLEH.month='m_2' AND FLEH.year = 'y_2'))
        AND FLH.fab_lot_hist_oid = FLEH.fab_lot_hist_oid
      WHERE SLW.parameter_name IN ('TOTAL_DEFECTIVE_DIE_BLANKET', 'TOTAL_DEFECTS_BLANKET', 'INSPECTION_FAILED_BLANKET')
      ORDER BY ch_id, SLW.sample_date"""

    val sqlStr1 = (sqlStr
      .replaceAll("last21day", last21day).replaceAll("last7day", last7day).replaceAll("today", today)
      .replaceAll("m_0", f"$m_0%02d").replaceAll("y_0", y_0.toString)
      .replaceAll("m_70", f"$m_70%02d").replaceAll("y_70", y_70.toString)
      .replaceAll("m_1", f"$m_1%02d").replaceAll("y_1", y_1.toString)
      .replaceAll("m_2", f"$m_2%02d").replaceAll("y_2", y_2.toString))
    val df = sqlContext.sql(sqlStr1)
    df.cache()

    // New method
    def toolEnd00 = udf((s1: String) => {
      s1.endsWith("00")
    })
    val df1 = df.filter(toolEnd00(df("equip_id"))).
      select(getConcatenated3(df("wis_ooc"), df("ch_id"), df("sample_id") as "key")
        , df("step_name")
        , strToIntstr(df("equip_id")) as "equip_id").
      rdd
    val pivot = df1.keyBy(r => r(0).toString).aggregateByKey(Map[String, String]())(
      (map1, row) => map1 + (row(1).toString -> row(2).toString),
      (map1, map2) => map1 ++ map2
    )
    // val keys = df2.map(r => r(0).toString).collect().distinct.length
    val columns = df1.map(r => r(1).toString).distinct.collect().sorted // distinct columns

    val result1 = pivot.map { case (k, map) =>
      Row.fromSeq(
        (k.split("\\|") ++ columns.map(c => if (map.contains(c)) map(c).toDouble else 0.0))
          .toSeq)
    }
    val schema1 = StructType(Array(StructField("wis_ooc", StringType), StructField("ch_id", StringType), StructField("sample_id", StringType))
      ++ columns.map(c => StructField(c, DoubleType)))
    val fin = sqlContext.createDataFrame(result1, schema1)
    fin.cache()
    val df_trav = fin.withColumn("label", wisToDouble1(fin("wis_ooc")))

    // select channel ids with at least 3 OOCs
    val a4 = df_trav.
      select(df_trav("ch_id"), df_trav("wis_ooc")).
      filter(df_trav("wis_ooc") === 1.0).groupBy("ch_id").count
    val ch_ids = a4.filter(a4("count") >= 3).
      map(r => r(0).toString).distinct.collect()

    val ignored = List("wis_ooc", "ch_id", "sample_id", "label")
    val columns1 = df_trav.columns
    val featInd = columns1.diff(ignored).map(columns1.indexOf(_))
    val ncol = featInd.length
    val targetInd = columns1.indexOf("label")
    val featureName = featInd.map(columns1(_))

    val imp_travelStep = ch_ids.map { k =>
      val a1 = df_trav.filter(df_trav("ch_id") === k)
      val a2 = a1.count() - a1.filter(a1("wis_ooc") === 1.0).count()

      if (a2 >= 3) {
        val b1 = a1.map { r =>
          val label = r.getDouble(targetInd)
          val nonnull_idx = featInd.filter(i => !r.isNullAt(i))
          val features = Vectors.sparse(
            ncol,
            nonnull_idx.map(i => i - 3),
            nonnull_idx.map(r.getDouble(_))
          )
          LabeledPoint(label, features)
        }
        (k, chiSqTest(b1).map(_.pValue))
      } else (k, List.fill(ncol)(10.0).toArray)
    }

    val impChanTrav = imp_travelStep.flatMap { case (k, pArray) =>
      val ind = (0 until ncol).filter(pArray(_) < 0.05)
      ind.map(i => k.toString + "|" + featureName(i))
    }
    impChanTrav.length
    //fin.unpersist()

    // After getting affecting traveler steps, highlight problematic tools
    def keyContain = udf((str: String) => {
      impChanTrav.contains(str)
    })
    val df2 = df.select(getConcatenated2(df("ch_id"), df("FLH.step_name")) as "key"
      , df("sample_id"), df("equip_id")
      , wisToDouble(df("wis_ooc")) as "wis_ooc")

    // find major tools contributing to OOC
    val df3_1 = df2.filter(keyContain(df2("key"))).filter(df2("wis_ooc") === 1.0)
    val df4_1 = df3_1.
      groupBy(df3_1("key")).
      agg(df3_1("key"), countDistinct(df3_1("sample_id")) as "total_1")
    val a5 = df3_1.
      select(df3_1("key") as "key1", df3_1("sample_id") as "sample_id1", df3_1("equip_id") as "equip_id1")
    val df5_1 = a5.
      groupBy(a5("key1"), a5("equip_id1")).
      agg(a5("key1"), a5("equip_id1"), countDistinct(a5("sample_id1")) as "no_1_per_tool")
    val divide = udf((x: Long, y: Long) => {
      x.toDouble / y
    })
    val a1 = df4_1.join(df5_1, df4_1("key") === df5_1("key1"))
    val df_1 = a1.
      filter(a1("total_1") >= 3).
      select(a1("key"), a1("equip_id1") as "equip_id"
        , divide(a1("no_1_per_tool"), a1("total_1")) as "perct_1")
    df_1.cache()

    val df3_2 = df2.filter(keyContain(df2("key"))).filter(df2("wis_ooc") === 0.0)
    val df4_2 = df3_2.
      groupBy(df3_2("key")).
      agg(df3_2("key"), countDistinct(df3_2("sample_id")) as "total_0")
    val a6 = df3_2.
      select(df3_2("key") as "key1", df3_2("sample_id") as "sample_id1", df3_2("equip_id") as "equip_id1")
    val df5_2 = a6.
      groupBy(a6("key1"), a6("equip_id1")).
      agg(a6("key1"), a6("equip_id1"), countDistinct(a6("sample_id1")) as "no_0_per_tool")

    val a2 = df4_2.join(df5_2, df4_2("key") === df5_2("key1"))
    val df_0 = a2.
      filter(a2("total_0") >= 3).
      select(a2("key") as "key0", a2("equip_id1") as "equip_id0"
        , divide(a2("no_0_per_tool"), a2("total_0")) as "perct_0")
    df_0.cache()

    // outer join to get all tools the process either wis 0.0 and 1.0
    val classify = udf((x: Double, y: Double) => {
      if (x > 0.8) {
        if (y > 0.8) {
          "Major_tool"
        } else if (x / y > 2) {
          "Bad_tool"
        } else {
          "Nothing"
        }
      }
      else if (x > 0.3 && x / y > 2) {
        "Bad_tool"
      }
      else if (x < 0.3 && x / y > 3) {
        "Bad_tool"
      }
      else {
        "Nothing"
      }
    })
    val a3 = df_1.join(df_0, df_1("key") === df_0("key0") and df_1("equip_id") === df_0("equip_id0"), "left_outer")
    val a7 = a3.
      select(a3("key"), a3("equip_id"), a3("perct_1"), a3("perct_0"),
        classify(a3("perct_1"), a3("perct_0")) as "tool_classified")
    val df_tool = a7.
      filter(a7("tool_classified") === "Major_tool" or a7("tool_classified") === "Bad_tool").
      orderBy(a7("key"), a7("equip_id"))
    df_tool.cache()

    // *******************************************************
    // Output
    // *******************************************************
    val getChId = udf((s1: String) => {
      s1.split("\\|")(0)
    })
    val getStep = udf((s1: String) => {
      s1.split("\\|")(1)
    })
    val m3 = df_tool.select(getChId(df_tool("key")) as "channel_id", getStep(df_tool("key")) as "step"
      , df_tool("equip_id") as "tool_name", df_tool("perct_1"), df_tool("perct_0"), df_tool("tool_classified"))
    /*val m2 = m1.map(a => Row(a(0), a(1), a(2)))
    val schema = StructType(Array(StructField("channel_id", StringType),
      StructField("step", StringType),
      StructField("tool_name", StringType)))
    val m3 = sqlContext.createDataFrame(m2, schema)*/

    sqlContext.sql("use user_mikenguyen")
    // write analysis
    val sqlStr2 =
      """CREATE TABLE IF NOT EXISTS user_mikenguyen.tb_wis_ooc_analysis_fast
        |(ch_id string, past_traveler_step string, tool string, perct_1 double
        |, perct_0 double, tool_classified string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t'
      """.stripMargin
    sqlContext.sql(sqlStr2)
    m3.insertInto("tb_wis_ooc_analysis_fast", true)

    // write combined result
    val sqlStr3 =
      """CREATE TABLE IF NOT EXISTS user_mikenguyen.tb_wis_ooc_result_fast
        |(wis_ooc string, ch_id string, part_type string, process_step_name string, parameter_name string, sample_id string
        |, lot_id string, parent_lot string, sample_date timestamp, raw_value double
        |, past_step_name string, equip_name string
        |, perct_1 double, perct_0 double, tool_classified string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t'
      """.stripMargin
    sqlContext.sql(sqlStr3)
    val m4 = m3.select("channel_id").distinct
    val df1_2 = df.join(m4, df("ch_id") === m4("channel_id"))
    val df2_2 = df1_2.
      select("wis_ooc", "ch_id", "PartType", "process_step_name", "parameter_name", "sample_id",
        "lot_id", "parent_lot", "sample_date", "raw_value", "step_name", "equip_id").
      join(m3, df1_2("ch_id") === m3("channel_id") and df1_2("step_name") === m3("step")
        and df1_2("equip_id") === m3("tool_name"), "left_outer")
    /*val df1_2 = df.join(m3, $"ch_id" === $"channel_id" &
      $"step_name" === $"step" &
      $"equip_id" === $"tool_name")*/
    df2_2.select("wis_ooc", "ch_id", "PartType", "process_step_name", "parameter_name", "sample_id",
      "lot_id", "parent_lot", "sample_date", "raw_value", "step_name", "equip_id",
      "perct_1", "perct_0", "tool_classified").
      insertInto("tb_wis_ooc_result_fast", true)
  }
}
