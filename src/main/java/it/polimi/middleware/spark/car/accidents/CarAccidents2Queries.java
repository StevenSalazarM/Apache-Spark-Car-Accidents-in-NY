package it.polimi.middleware.spark.car.accidents;

import static org.apache.spark.sql.functions.*;

//import static org.apache.spark.sql.functions.weekofyear;

//import static org.apache.spark.sql.functions.to_date;
//import static org.apache.spark.sql.functions.year;

//import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import it.polimi.middleware.spark.tutorial.utils.LogUtils;

public class CarAccidents2Queries {
	public static void main(String[] args) {
		LogUtils.setLogLevel();

		final String master = args.length > 0 ? args[0] : "local[*]";
		final String filePath = args.length > 1 ? args[1] : "./files/";
		final String testNumber = args.length > 2 ? args[2] : "0";
		final String cache = args.length > 3 ? args[3] : "true";
		final long test_id = System.currentTimeMillis();
		final SparkSession spark = SparkSession //
				.builder() //
				.master(master) //
				.appName("Car Accidents in New York") //
				.getOrCreate();

		final List<StructField> mySchemaFields = new ArrayList<>();
		mySchemaFields.add(DataTypes.createStructField("DATE", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("TIME", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("BOROUGH", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("ZIP CODE", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("LATITUDE", DataTypes.DoubleType, true));
		mySchemaFields.add(DataTypes.createStructField("LONGITUDE", DataTypes.DoubleType, true));
		mySchemaFields.add(DataTypes.createStructField("LOCATION", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("ON STREET NAME", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CROSS STREET NAME", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("OFF STREET NAME", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PERSONS INJURED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PERSONS KILLED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PEDESTRIANS INJURED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PEDESTRIANS KILLED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF CYCLIST INJURED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF CYCLIST KILLED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF MOTORIST INJURED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF MOTORIST KILLED", DataTypes.ShortType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 1", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 2", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 3", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 4", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 5", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("UNIQUE KEY", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 1", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 2", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 3", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 4", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 5", DataTypes.StringType, true));

		final StructType mySchema = DataTypes.createStructType(mySchemaFields);

		final Dataset<Row> ds = spark //
				.read() //
				.option("header", "true") //
				.option("delimiter", ",").option("inferSchema", "false").schema(mySchema)//
				.csv(filePath + "NYPD_Motor_Vehicle_Collisions.csv");

		final Dataset<Row> ds_with_correct_nums = ds.withColumn("TOTAL_I",
																ds.col("NUMBER OF PEDESTRIANS INJURED")
																		.plus(ds.col("NUMBER OF CYCLIST INJURED")
																				.plus(ds.col("NUMBER OF MOTORIST INJURED"))))
													.drop("NUMBER OF PEDESTRIANS INJURED").drop("NUMBER OF CYCLIST INJURED")
													.drop("NUMBER OF MOTORIST INJURED").withColumnRenamed("NUMBER OF PERSONS INJURED", "FAKETOTAL_I")
										
													.withColumn("TOTAL_K",
																ds.col("NUMBER OF PEDESTRIANS KILLED")
																		.plus(ds.col("NUMBER OF CYCLIST KILLED")
																				.plus(ds.col("NUMBER OF MOTORIST KILLED"))))
													.drop("NUMBER OF PEDESTRIANS KILLED").drop("NUMBER OF CYCLIST KILLED").drop("NUMBER OF MOTORIST KILLED")
													.withColumnRenamed("NUMBER OF PERSONS KILLED", "FAKETOTAL_K")
									
													// for Query 2, it will be useful to change the name the of CONTRIBUTING FACTORS
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1", "C1")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 2", "C2")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 3", "C3")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 4", "C4")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 5", "C5");

		final Dataset<Row> ds_corrected = ds_with_correct_nums.filter(ds_with_correct_nums.col("FAKETOTAL_I").equalTo(ds_with_correct_nums.col("TOTAL_I"))
																		.and(ds_with_correct_nums.col("FAKETOTAL_K").equalTo(ds_with_correct_nums.col("TOTAL_K"))));
		
		if(cache.equals("true")) {ds_corrected.cache();System.out.println("Cache enabled");}

		final Dataset<Row> ds_lethal_accidents = ds_corrected.filter(ds_corrected.col("TOTAL_K").gt(0));

		final Dataset<Row> q1 = ds_lethal_accidents.withColumn("WEEK", weekofyear(to_date(ds_lethal_accidents.col("DATE"), "MM/dd/yyyy")))
															   .withColumn("YEAR", year(to_date(ds_lethal_accidents.col("DATE"), "MM/dd/yyyy")))
															   .groupBy("YEAR", "WEEK").agg(count("TOTAL_K").as("N. LETHAL ACCIDENTS"));
		
		
		q1.write().format("csv").save(filePath + testNumber + "_" + String.valueOf(test_id) + "/query1/");

		
		// the following queries are just a copy of the first one		
		final Dataset<Row> ds_lethal_accidents2 = ds_corrected.filter(ds_corrected.col("TOTAL_K").gt(0));
		final Dataset<Row> q2 = ds_lethal_accidents2.withColumn("WEEK", weekofyear(to_date(ds_lethal_accidents2.col("DATE"), "MM/dd/yyyy")))
																 .withColumn("YEAR", year(to_date(ds_lethal_accidents2.col("DATE"), "MM/dd/yyyy")))
																 .groupBy("YEAR", "WEEK").agg(count("TOTAL_K").as("N. LETHAL ACCIDENTS"));

		q2.write().format("csv").save(filePath + testNumber + "_" + String.valueOf(test_id) + "/query2/");

		if(cache.equals("true"))ds_corrected.unpersist();

	}
}

