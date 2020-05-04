package it.polimi.middleware.spark.car.accidents;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import it.polimi.middleware.spark.tutorial.utils.LogUtils;


public class LoadTimeNoCache {
	public static void main(String[] args) {
		LogUtils.setLogLevel();

		final String master = args.length > 0 ? args[0] : "local[*]";
		final String filePath = args.length > 1 ? args[1] : "./files/";
		final SparkSession spark = SparkSession //
		    .builder() //
		    .master(master) //
		    .appName("Load Time (no cache version)") //
		    .getOrCreate();

		
		
		// An static schema is useful since we are dealing with a csv file
		// and an inferSchema would need to read the whole file to decide the correct types.
		// Also, even if we have the first row with the names of the columns it does not give any information about the types
		// so using only header=yes would make all the columns of StringType and we would need to cast each numeric value
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
			    .option("delimiter", ",")
			    .option("inferSchema", "false")
			    .schema(mySchema)//
			    .csv(filePath+"NYPD_Motor_Vehicle_Collisions.csv");
		

	// FILTERING INCORRECT VALUES: each NUM_PERSON must be equal to SUM(N. OF PEDESTRIANS, N. OF CYCLIST, N. OF MOTORIST)
		
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
													
													// for Query 2, it will be useful to change the name the of CONTRIBUTING FACTORS
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1", "C1")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 2", "C2")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 3", "C3")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 4", "C4")
													.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 5", "C5")
													;

		// We should filter the data set and take only accidents that have the correct values
		final Dataset<Row> ds_corrected = ds_with_correct_nums.filter(
											ds_with_correct_nums.col("FAKETOTAL_I").equalTo(ds_with_correct_nums.col("TOTAL_I")).and(
											ds_with_correct_nums.col("FAKETOTAL_K").equalTo(ds_with_correct_nums.col("TOTAL_K"))))
											;
		ds_corrected.count();
		
	}
}

