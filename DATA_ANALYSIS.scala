import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession,SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DATA_ANALYSIS {

  val user = "root";
  val password = "Dhanush5"
  val spark = SparkSession.builder().appName("makaan_housing_analysis").config("spark.master", "local[*]").getOrCreate()

  def read_table(table: String): DataFrame = {
    val DF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/BDMS")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", table)
      .option("user", user)
      .option("password", password).load()
    return DF
  }

  def write_table(table: String,df:DataFrame): Unit= {
      df.write.format("jdbc").option("url", "jdbc:mysql://localhost/ANALYSIS")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .mode("overwrite")
      .save()
  }

  def preprocessing(df: DataFrame): DataFrame = {
    df.columns
    val filteredDF = df.filter(!(df.columns.map(c => col(c).isNull || trim(col(c)) === "").reduce(_ || _)))
    return filteredDF
  }

// Dhanush
  def population_based_on_bhk(df1: DataFrame, df2: DataFrame): Unit = {
    val df = df1.join(df2, df1("locality_id") === df2("locality_id"))
    val df3 = df.withColumn("no_of_people_in_house", regexp_extract(col("number_of_bhk"), "(\\d+)", 1).cast("integer") * 4)
      .groupBy(col("city_name")).agg(sum("no_of_people_in_house").
      as("population_per_locality"))
      .select(col("city_name"),
        (col("population_per_locality") * 100).divide(df.withColumn("no_of_people_in_house", regexp_extract(col("number_of_bhk"), "(\\d+)", 1)
          .cast("integer") * 4).agg(sum("no_of_people_in_house")).collect()(0){0})
          .as("population_percentage"))
      .orderBy(col("population_percentage").desc).select("*")
    write_table("population_spread", df3)
    df3.show()
  }

  def increase_in_price_over_time(df: DataFrame): Unit = {
    val df1 = df.withColumn("time_column", when(col("posted_on").like("%day%"),
      regexp_extract(col("number_of_bhk"), "(\\d+)", 1).cast("integer")))
      .filter(!col("time_column").isNull)

    val avgPriceDF = df1
      .groupBy("property_type", "time_column")
      .agg(avg("price_per_unit_area").alias("avg_price"))
      .orderBy("property_type", "time_column")
    avgPriceDF.show()

    val windowSpec = Window.partitionBy("property_type").orderBy("time_column")

    val trendChangeDF = avgPriceDF
      .withColumn("prev_avg_price", lead("avg_price", 1).over(windowSpec))
      .withColumn("price_change", when(col("prev_avg_price").isNull, null)
        .otherwise((((col("avg_price") - col("prev_avg_price")) / col("prev_avg_price")) * 100).cast("decimal(10,2)")))
      .orderBy("property_type", "time_column").select("*")
    trendChangeDF.show()
//    write_table("price_trend_over_month", trendChangeDF)

  }

  def locality_type_classification(df1: DataFrame, df2: DataFrame): Unit = {
    val df = df1.join(df2, df1("locality_id") === df2("locality_id"))
    val win_fun = Window.partitionBy("locality_name").orderBy("price")
    val win_fun1 = Window.partitionBy("locality_name")
    df.withColumn("rank", row_number.over(win_fun)).withColumn("average_price_per_cent", avg("price_per_unit_area").over(win_fun1))
      .withColumn("locality_type",
        when(col("distance_from_airport") < 50 and col("distance_from_railway") < 20 and col("average_price_per_cent") > 15000, "1 -> upper_class")
          .when(col("distance_from_airport") < 50 and col("distance_from_railway") < 20, "2 -> mid_class")
          .when(col("distance_from_railway") < 35, "3 -> lower_class")
          .otherwise("4 -> Village")).filter(col("rank") < 2)
      .select("locality_type", "locality_name", "distance_from_airport", "distance_from_railway", "average_price_per_cent")
      .orderBy("locality_type", "average_price_per_cent").show()
  }

//Srikar
  def properties_span_over_localites(df1: DataFrame, df2: DataFrame): Unit = {
    val df = df1.join(df2, df1("suburban_id") === df2("suburban_id"))
    val win_fun = Window.partitionBy("suburban_name", "property_type").orderBy("size")
    val win_fun1 = Window.partitionBy("suburban_name", "property_type")
    df.withColumn("row_rank", row_number.over(win_fun)).withColumn("area_span_in_sqft",
      sum(regexp_replace(split(col("size"), " ").getItem(0), ",", "")
        .cast("integer")).over(win_fun1))
      .withColumn("no_of_properties", count("size").over(win_fun1))
      .where(col("row_rank") === 1)
      .select("suburban_name", "property_type", "area_span_in_sqft", "no_of_properties")
      .orderBy("suburban_name", "property_type", "area_span_in_sqft").show()
  }

  def builder_profile_analysis(df1: DataFrame, df2: DataFrame): Unit = {
    val df = df1.join(df2, df1("builder_id") === df2("builder_id"))
    val df3 = df.groupBy("builder_name", "builder_rating").agg(count("property_id").as("no_of_properties_designed"),
      avg("price").cast("integer").as("average_price_charged"))
      .withColumn("builder_status",
        when((col("no_of_properties_designed") > 50) and (col("builder_rating") > 3), "experienced_builder")
          .when((col("no_of_properties_designed") > 5) and (col("builder_rating") > 2), "upcoming_builder")
          .otherwise("rookie_builder"))
      .orderBy(col("no_of_properties_designed").desc, col("average_price_charged")).select("*")
    df3.show()
    write_table("builders_analysis", df3)
  }

  def average_price_of_property_for_locality(df1: DataFrame, df2: DataFrame) = {
    val df = df1.join(df2, df1("locality_id") === df2("locality_id"))
    val win_fun1 = Window.partitionBy("locality_name", "property_type").orderBy("price")
    val win_fun2 = Window.partitionBy("locality_name", "property_type")
    val df3=df.withColumn("row_rank", row_number.over(win_fun1))
      .withColumn("average_price", avg(col("price"))
        .over(win_fun2).cast("integer")).where(col("row_rank") === 1)
      .select("locality_name", "property_type", "average_price")
    df3.show()
    write_table("property_price_per_locality",df3)
    spark.stop()
  }

  //Anand

  def allowed_bargain_percentage(df1: DataFrame, df2: DataFrame, df3: DataFrame): Unit = {
    val dfn = df1.join(df2, df1("locality_id") === df2("locality_id"))
    val df = dfn.join(df3, dfn("builder_id") === df3("builder_id"))
    val win_fun1 = Window.partitionBy("city_name", "property_type", "number_of_bhk")
    df.withColumn("maximum_bargain_limit", ((col("price") - min("price").over(win_fun1))
      .divide(col("price")) * (lit(100) / (lit(4) * (lit(6) - col("builder_rating"))))))
      .select("property_id","city_name", "property_type", "number_of_bhk", "price", "maximum_bargain_limit")
      .orderBy(col("maximum_bargain_limit").desc)
      .show()
  }

  def investment_per_city(df1:DataFrame,df2:DataFrame): Unit = {
    val df = df1.join(df2, df1("locality_id") === df2("locality_id"))
    val df3 = df.withColumn("year",
      when((col("posted_on").like("%month%")) or (col("posted_on").like("%day%")),"This_year").
        when((col("posted_on").like("%year%")) and
          ((col("posted_on").like("%1%")) or (col("posted_on").like("%a%")) ),"Last_year")
        .otherwise("Before_last_year")).groupBy("year","city_name").agg(sum("price").as("investment"))
    df3.show()
    write_table("investment_per_city",df3)
  }

  //Sudiksha

  def average_price_per_bhk(df1: DataFrame, df2: DataFrame) = {
    val df = df1.join(df2, df1("suburban_id") === df2("suburban_id"))
    val win_fun1 = Window.partitionBy("suburban_name", "number_of_bhk", "is_furnished").orderBy("price")
    val win_fun2 = Window.partitionBy("suburban_name", "number_of_bhk", "is_furnished")
    val df3 = df.withColumn("row_rank", row_number.over(win_fun1))
      .withColumn("average_price", avg(col("price"))
        .over(win_fun2).cast("integer")).where(col("row_rank") === 1)
      .select("suburban_name", "number_of_bhk", "is_furnished", "average_price")
    df3.show()
    write_table("bhk_price_per_locality", df3)
  }


  def hike_in_price(df1: DataFrame, df2: DataFrame): Unit = {
    val df = df1.join(df2, df1("locality_id") === df2("locality_id"))
    val under_con_df = df.where(col("property_status").like("%Under%")).distinct()
    val ready_to_mov = df.where(col("property_status").like("%Ready%"))
      .select(col("is_furnished").as("is_furnished2"), col("price").as("price2")
        , col("property_status").as("property_status2"), col("property_id")).distinct()

    under_con_df.join(ready_to_mov, under_con_df("property_id") === ready_to_mov("property_id"), "inner")
      .withColumn("hike_in_price", col("price2") - col("price"))
      .withColumn(("change_in_furniture"), when(!(col("is_furnished") === col("is_furnished2")), "Yes").otherwise("No"))
      .select("property_name", "property_type", "project_url", "city_name", "price", "price2", "hike_in_price", "change_in_furniture")
      .show()
  }
  //visualization:
  def geospatial_visualization(df: DataFrame): Unit = {
    val df3=df.withColumn("demand",
      when(col("price_per_unit_area") > 15000, "v").
        when(col("price_per_unit_area") > 10000, "i").
        when(col("price_per_unit_area") > 8000, "b").
        when(col("price_per_unit_area") > 6000, "g").
        when(col("price_per_unit_area") > 4000, "y").
        when(col("price_per_unit_area") > 2000, "o").otherwise("r")).select("latitude", "longitude", "demand")
    write_table("geospatial_visualization",df3)
    df3.show()
  }

  def main(args: Array[String]): Unit = {
    val makaan_df = read_table("makaan_estates")
    val builder_df = read_table("builder_details")
    val locality_df = read_table("locality_details")
    val suburban_df1 = read_table("suburban_details")
    val suburban_df = suburban_df1.withColumn("suburban_name",
      split(col("suburban_name") ,"\r").getItem(0) )

    //Dhanush
//           population_based_on_bhk(makaan_df,locality_df)        //plot done
//           increase_in_price_over_time(makaan_df)                //plot done
//           locality_type_classification(makaan_df,locality_df)
//
//   // Srikar
//            builder_profile_analysis(makaan_df,builder_df)   //plot done
//            properties_span_over_localites(makaan_df,suburban_df) //split done
//            average_price_of_property_for_locality(preprocessing(makaan_df),preprocessing(locality_df));     //plot done


    //Anand
            allowed_bargain_percentage(makaan_df, locality_df, builder_df)
//            investment_per_city(makaan_df,locality_df)

    //Sudiksha
//            average_price_per_bhk(makaan_df,suburban_df);       //plot done
//            hike_in_price(makaan_df,locality_df)
//            geospatial_visualization(makaan_df)

  }

}
