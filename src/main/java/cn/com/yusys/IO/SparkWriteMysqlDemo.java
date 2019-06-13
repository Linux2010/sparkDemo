package cn.com.yusys.IO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/*********************************
 * @Created on 2018-6-2.   ******
 * @Author: tianfusheng    ******
 * @Version: 1.0          ******
 ********************************/
public class SparkWriteMysqlDemo {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir", "D:\\hadoopbin\\");
        String master = "local";
        String appName = "SparkJdbcDemo";
        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores","1")
                .getOrCreate();

        JavaSparkContext sparkContext =new JavaSparkContext(spark.sparkContext());
        SQLContext sqlContext = new SQLContext(sparkContext);
        JavaRDD<String> personData = sparkContext.parallelize(Arrays.asList("1 tom 5", "2 jim 6", "3 hh 9", "4 zz 10", "5 yy 1"));
        JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(Integer.valueOf(splited[0]),splited[1],Integer.valueOf(splited[2]));
            }
        });
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbtable", "PERSON_INFO");
        connectionProperties.setProperty("user", "root");
        connectionProperties.setProperty("password","root");
        connectionProperties.setProperty("driver","com.mysql.jdbc.Driver");

        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> persistDF = sqlContext.createDataFrame(personsRDD,structType);
        persistDF.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://172.16.60.41:3306/source","PERSON_INFO",connectionProperties);
    }
}
