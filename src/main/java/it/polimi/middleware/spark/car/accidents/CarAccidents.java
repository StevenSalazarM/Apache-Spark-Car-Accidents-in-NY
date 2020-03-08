package it.polimi.middleware.spark.car.accidents;

import static org.apache.spark.sql.functions.*;

//import static org.apache.spark.sql.functions.weekofyear;

//import static org.apache.spark.sql.functions.to_date;
//import static org.apache.spark.sql.functions.year;

//import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Functions;

import it.polimi.middleware.spark.tutorial.utils.LogUtils;

/**
 * Bank example
 *
 * Input: csv files with list of deposits and withdrawals, having the following
 * schema ("person: String, account: String, amount: Int)
 *
 * Two queries Q1. Print the person with the maximum total amount of withdrawals
 * Q2. Print all the accounts with a negative balance
 *
 * The code exemplifies the use of SQL primitives
 *
 * Version 2. Exemplifies the benefits of persisting intermediate results to
 * enable reuse.
 */
public class CarAccidents {
	public static void main(String[] args) {
		LogUtils.setLogLevel();

		final String master = args.length > 0 ? args[0] : "local[4]";
		final String filePath = args.length > 1 ? args[1] : "./";

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
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PERSONS INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PERSONS KILLED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PEDESTRIANS INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PEDESTRIANS KILLED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF CYCLIST INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF CYCLIST KILLED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF MOTORIST INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF MOTORIST KILLED", DataTypes.IntegerType, true));
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
			    .option("delimiter", ",")
			    .option("inferSchema", "false")
			    .schema(mySchema)//
			    .csv(filePath + "files/sample.csv");
			
		ds.printSchema();
		
		
	// FILTERING OF INCORRECT VALUES NUM_PERSON must be equals to SUM(N OF PEDESTRIANS, N_OF_CYCLIST, N_OF_MOTORIST)
		
		// First we create a column that contains the real number of persons killed 
		final Dataset<Row> ds_with_correct_nums = ds.withColumn("TOTAL_I",ds.col("NUMBER OF PEDESTRIANS INJURED")
																		.plus(ds.col("NUMBER OF CYCLIST INJURED")
																		.plus(ds.col("NUMBER OF MOTORIST INJURED"))))
													.drop("NUMBER OF PEDESTRIANS INJURED")
													.drop("NUMBER OF CYCLIST INJURED")
													.drop("NUMBER OF MOTORIST INJURED")
													.withColumnRenamed("NUMBER OF PERSONS INJURED", "FAKETOTAL_I")		
													
													.withColumn("TOTAL_K",ds.col("NUMBER OF PEDESTRIANS KILLED")
																		.plus(ds.col("NUMBER OF CYCLIST KILLED")
																		.plus(ds.col("NUMBER OF MOTORIST KILLED"))))
													.drop("NUMBER OF PEDESTRIANS KILLED")
													.drop("NUMBER OF CYCLIST KILLED")
													.drop("NUMBER OF MOTORIST KILLED")
													.withColumnRenamed("NUMBER OF PERSONS KILLED", "FAKETOTAL_K")
													;

		
		// We should filter the data set and take only accidents that have the correct values
		final Dataset<Row> ds_corrected = ds_with_correct_nums.filter(
											ds_with_correct_nums.col("FAKETOTAL_I").equalTo(ds_with_correct_nums.col("TOTAL_I")).and(
											ds_with_correct_nums.col("FAKETOTAL_K").equalTo(ds_with_correct_nums.col("TOTAL_K"))))
											;
		
		
		
	// Q1 Number of lethal accidents per week throughout the entire dataset
		
		final Dataset<Row> ds_lethal_accidents = ds_corrected.filter(ds_corrected.col("TOTAL_K").gt(0));
		ds_lethal_accidents.show();
				
		// We are interested in group by YEAR and WEEK so we should create those two columns and sum the total number of accidents
		final Dataset<Row> ds_le_per_week = ds_lethal_accidents.withColumn("WEEK",weekofyear(to_date(ds_lethal_accidents.col("DATE"),"MM/dd/yyyy")))
												.withColumn("YEAR", year(to_date(ds_lethal_accidents.col("DATE"),"MM/dd/yyyy")))
												.groupBy("YEAR","WEEK").sum("TOTAL_K");
		
		
		final Dataset<Row> q1=ds_le_per_week.withColumnRenamed("sum(TOTAL_I)", "N. LETHAL ACCIDENTS");
		
		q1.show();
		
	// Q2 Number  of  accidents  and  percentage  of  number  of  deaths  per  contributing  factor  in  the data set
		

		Scanner a = new Scanner(System.in);
		a.nextLine();
		spark.close();

	}
}
