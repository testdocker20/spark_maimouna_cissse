import org.apache.spark.sql.{SparkSession, functions} 
import org.apache.spark.sql.Row 
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, FloatType} 
import org.apache.log4j.Logger 
import org.apache.log4j.Level 

object App {
      
      def main(args: Array[String]):Unit={
        println("Hello, world!")
         val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()
         val communes_df = spark.read.option("header",true).option("delimiter", 
";").csv("/home/mouna/esgi_tp/Communes.csv")
         communes_df.show()
         communes_df.printSchema()
         val cp_df = spark.read.option("header",true).option("delimiter", 
";").csv("/home/mouna/esgi_tp/code-insee-postaux-geoflar.csv")
         cp_df.show()
         cp_df.printSchema()
         val poste_synop_df = spark.read.option("header",true).option("delimiter", 
";").csv("/home/mouna/esgi_tp/postesSynop.txt")
         poste_synop_df.show()
         poste_synop_df.printSchema()
         val synop_df = spark.read.option("header",true).option("delimiter", 
";").csv("/home/mouna/esgi_tp/synop.2020120512.txt")
         synop_df.show()
         synop_df.printSchema()
         //csv_df_v2.write.parquet("csv_df_v2_parquet_file.parquet")
     }
}
