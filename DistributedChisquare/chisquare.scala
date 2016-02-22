/**
 * Created by mikenguyen on 1/29/2016.
 */
// databases and variables have been masked

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

    val sqlStr = """SELECT DISTINCT SV.wo, cast(SC.ch as String) ch, SLW.e_01 pt, SLW.e_06 as step, SLW.pr
            , cast(SLW.sample as String) sample, SLW.e_12 as lid, regexp_replace(SLW.e_12, '\\.0\\p{Alnum}', '\\.00') as parent
            , SLW.sd, SR.raw_value
            , FLH.sn, FLEH.ei
      FROM (SELECT sample, pr, sd, e_01, e_06, e_12, month FROM data.sw a
            WHERE (month = 'm_0' or month = 'm_70')
            AND sd > cast('last7day' as timestamp)
            UNION ALL
            SELECT sample, pr, sd, e_01, e_06, e_12, month FROM data.sl b
            WHERE (month = 'm_0' or month = 'm_70')
            AND sd > cast('last7day' as timestamp)
            ) SLW
      INNER JOIN (SELECT sample, raw_value, month FROM data.sr1
            WHERE (month = 'm_0' or month = 'm_70')
            UNION ALL
            SELECT sample, max(raw_value) as raw_value, month FROM data.sr2
            WHERE (month = 'm_0' or month = 'm_70')
            GROUP BY sample, month
            ) SR
        ON SR.sample = SLW.sample
      INNER JOIN (SELECT ch, sample, out_of_order, ckc_id, month FROM data.sc
            WHERE month = 'm_0' or month = 'm_70'
            AND out_of_order = 'N'
            AND ckc_id = 0) SC
        ON SC.sample = SLW.sample
      LEFT JOIN (SELECT DISTINCT sd,ch as sv_ch, sample,'1' AS wo,month
            FROM data.isv) SV
        ON (SLW.sample = SV.sample and SV.sv_ch = SC.ch)
      INNER JOIN ftrkg.flhy FLH
        ON ((FLH.month='m_0' and FLH.year = 'y_0') or (FLH.month='m_1' and FLH.year = 'y_1') or (FLH.month='m_2' AND FLH.year = 'y_2'))
        AND FLH.lid = regexp_replace(SLW.e_12, '\\.0\\p{Alnum}', '\\.00')
        AND FLH.tro_datetime > date_sub(SLW.sd, 4)
        AND FLH.tro_datetime < SLW.sd
        AND FLH.sn not like '1%'
      INNER JOIN ftrkg.flehy FLEH
        ON ((FLEH.month='m_0' and FLEH.year = 'y_0') or (FLEH.month='m_1' and FLEH.year = 'y_1') or (FLEH.month='m_2' AND FLEH.year = 'y_2'))
        AND FLH.flho = FLEH.flho
      ORDER BY ch, SLW.sd"""

    val sqlStr1 = (sqlStr
      .replaceAll("last21day", last21day).replaceAll("last7day", last7day).replaceAll("today", today)
      .replaceAll("m_0", f"$m_0%02d").replaceAll("y_0", y_0.toString)
      .replaceAll("m_70", f"$m_70%02d").replaceAll("y_70", y_70.toString)
      .replaceAll("m_1", f"$m_1%02d").replaceAll("y_1", y_1.toString)
      .replaceAll("m_2", f"$m_2%02d").replaceAll("y_2", y_2.toString))
    val df = sqlContext.sql(sqlStr1)
    df.cache()

    // New method
    def tEnd00 = udf((s1: String) => {
      s1.endsWith("00")
    })
    val df1 = df.filter(tEnd00(df("ei"))).
      select(getConcatenated3(df("wo"), df("ch"), df("sample") as "key")
        , df("sn")
        , strToIntstr(df("ei")) as "ei").
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
    val schema1 = StructType(Array(StructField("wo", StringType), StructField("ch", StringType), StructField("sample", StringType))
      ++ columns.map(c => StructField(c, DoubleType)))
    val fin = sqlContext.createDataFrame(result1, schema1)
    fin.cache()
    val df_trav = fin.withColumn("label", wisToDouble1(fin("wo")))

    // select channel ids with at least 3 OOCs
    val a4 = df_trav.
      select(df_trav("ch"), df_trav("wo")).
      filter(df_trav("wo") === 1.0).groupBy("ch").count
    val chs = a4.filter(a4("count") >= 3).
      map(r => r(0).toString).distinct.collect()

    val ignored = List("wo", "ch", "sample", "label")
    val columns1 = df_trav.columns
    val featInd = columns1.diff(ignored).map(columns1.indexOf(_))
    val ncol = featInd.length
    val targetInd = columns1.indexOf("label")
    val featureName = featInd.map(columns1(_))

    val imp_travelStep = chs.map { k =>
      val a1 = df_trav.filter(df_trav("ch") === k)
      val a2 = a1.count() - a1.filter(a1("wo") === 1.0).count()

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

    // After getting affecting traveler steps, highlight problematic ts
    def keyContain = udf((str: String) => {
      impChanTrav.contains(str)
    })
    val df2 = df.select(getConcatenated2(df("ch"), df("FLH.sn")) as "key"
      , df("sample"), df("ei")
      , wisToDouble(df("wo")) as "wo")

    // find major ts contributing to OOC
    val df3_1 = df2.filter(keyContain(df2("key"))).filter(df2("wo") === 1.0)
    val df4_1 = df3_1.
      groupBy(df3_1("key")).
      agg(df3_1("key"), countDistinct(df3_1("sample")) as "total_1")
    val a5 = df3_1.
      select(df3_1("key") as "key1", df3_1("sample") as "sample1", df3_1("ei") as "ei1")
    val df5_1 = a5.
      groupBy(a5("key1"), a5("ei1")).
      agg(a5("key1"), a5("ei1"), countDistinct(a5("sample1")) as "no_1_per_t")
    val divide = udf((x: Long, y: Long) => {
      x.toDouble / y
    })
    val a1 = df4_1.join(df5_1, df4_1("key") === df5_1("key1"))
    val df_1 = a1.
      filter(a1("total_1") >= 3).
      select(a1("key"), a1("ei1") as "ei"
        , divide(a1("no_1_per_t"), a1("total_1")) as "perct_1")
    df_1.cache()

    val df3_2 = df2.filter(keyContain(df2("key"))).filter(df2("wo") === 0.0)
    val df4_2 = df3_2.
      groupBy(df3_2("key")).
      agg(df3_2("key"), countDistinct(df3_2("sample")) as "total_0")
    val a6 = df3_2.
      select(df3_2("key") as "key1", df3_2("sample") as "sample1", df3_2("ei") as "ei1")
    val df5_2 = a6.
      groupBy(a6("key1"), a6("ei1")).
      agg(a6("key1"), a6("ei1"), countDistinct(a6("sample1")) as "no_0_per_t")

    val a2 = df4_2.join(df5_2, df4_2("key") === df5_2("key1"))
    val df_0 = a2.
      filter(a2("total_0") >= 3).
      select(a2("key") as "key0", a2("ei1") as "ei0"
        , divide(a2("no_0_per_t"), a2("total_0")) as "perct_0")
    df_0.cache()

    // outer join to get all ts the process either wis 0.0 and 1.0
    val classify = udf((x: Double, y: Double) => {
      if (x > 0.8) {
        if (y > 0.8) {
          "Major_t"
        } else if (x / y > 2) {
          "Bad_t"
        } else {
          "Nothing"
        }
      }
      else if (x > 0.3 && x / y > 2) {
        "Bad_t"
      }
      else if (x < 0.3 && x / y > 3) {
        "Bad_t"
      }
      else {
        "Nothing"
      }
    })
    val a3 = df_1.join(df_0, df_1("key") === df_0("key0") and df_1("ei") === df_0("ei0"), "left_outer")
    val a7 = a3.
      select(a3("key"), a3("ei"), a3("perct_1"), a3("perct_0"),
        classify(a3("perct_1"), a3("perct_0")) as "t_classified")
    val df_t = a7.
      filter(a7("t_classified") === "Major_t" or a7("t_classified") === "Bad_t").
      orderBy(a7("key"), a7("ei"))
    df_t.cache()

    // *******************************************************
    // Output
    // *******************************************************
    val getChId = udf((s1: String) => {
      s1.split("\\|")(0)
    })
    val getStep = udf((s1: String) => {
      s1.split("\\|")(1)
    })
    val m3 = df_t.select(getChId(df_t("key")) as "cid", getStep(df_t("key")) as "step"
      , df_t("ei") as "tn", df_t("perct_1"), df_t("perct_0"), df_t("t_classified"))
    /*val m2 = m1.map(a => Row(a(0), a(1), a(2)))
    val schema = StructType(Array(StructField("cid", StringType),
      StructField("step", StringType),
      StructField("tn", StringType)))
    val m3 = sqlContext.createDataFrame(m2, schema)*/

    sqlContext.sql("use user_mikenguyen")
    // write analysis
    val sqlStr2 =
      """CREATE TABLE IF NOT EXISTS user_mikenguyen.two
        |(ch string, past_ts string, t string, perct_1 double
        |, perct_0 double, t_classified string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t'
      """.stripMargin
    sqlContext.sql(sqlStr2)
    m3.insertInto("two", true)

    // write combined result
    val sqlStr3 =
      """CREATE TABLE IF NOT EXISTS user_mikenguyen.rlt
        |(wo string, ch string, part_type string, step string, pr string, sample string
        |, lid string, parent string, sd timestamp, raw_value double
        |, past_sn string, equip_name string
        |, perct_1 double, perct_0 double, t_classified string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t'
      """.stripMargin
    sqlContext.sql(sqlStr3)
    val m4 = m3.select("cid").distinct
    val df1_2 = df.join(m4, df("ch") === m4("cid"))
    val df2_2 = df1_2.
      select("wo", "ch", "pt", "step", "pr", "sample",
        "lid", "parent", "sd", "raw_value", "sn", "ei").
      join(m3, df1_2("ch") === m3("cid") and df1_2("sn") === m3("step")
        and df1_2("ei") === m3("tn"), "left_outer")
    /*val df1_2 = df.join(m3, $"ch" === $"cid" &
      $"sn" === $"step" &
      $"ei" === $"tn")*/
    df2_2.select("wo", "ch", "pt", "step", "pr", "sample",
      "lid", "parent", "sd", "raw_value", "sn", "ei",
      "perct_1", "perct_0", "t_classified").
      insertInto("tb_wo_result_fast", true)
  }
}
