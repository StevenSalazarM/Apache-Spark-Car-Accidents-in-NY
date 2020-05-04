package it.polimi.middleware.spark.car.accidents;

import static org.apache.spark.sql.functions.*;

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


public class CarAccidentsCache {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./files/";
        final String testNumber = args.length > 2 ? args[2] : "0";
        final long test_id = System.currentTimeMillis();
        final SparkSession spark = SparkSession //
            .builder() //
            .master(master) //
            .appName("Car Accidents in New York (cache version)") //
            .getOrCreate();

        
        
        // An static schema is useful since we are dealing with a csv file
        // and an inferSchema would need to read the whole file to decide the correct types.
        // Also, even if we have the first row with the names of the columns we do not have any information about the types
        // so using only header=yes would make all the columns of StringType and we would need to cast each numeric value for sum and average
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
        							
        
        // First we create a column that contains the real number of persons injured and persons killed
        final Dataset<Row> ds_with_correct_nums = ds.withColumn("TOTAL_I",ds.col("NUMBER OF PEDESTRIANS INJURED")
                                                                        .plus(ds.col("NUMBER OF CYCLIST INJURED")
                                                                        .plus(ds.col("NUMBER OF MOTORIST INJURED"))))
                                                    // now we dont need to use the following three columns
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
                                                    
                                                    // for Query 2, it will be easier to refer to each contributing factor as CX
                                                    .withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1", "C1")
                                                    .withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 2", "C2")
                                                    .withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 3", "C3")
                                                    .withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 4", "C4")
                                                    .withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 5", "C5")
                                                    ;

        // We should filter the dataset and take only accidents that have the correct values
        final Dataset<Row> ds_corrected = ds_with_correct_nums.filter(
                                            ds_with_correct_nums.col("FAKETOTAL_I").equalTo(ds_with_correct_nums.col("TOTAL_I")).and(
                                            ds_with_correct_nums.col("FAKETOTAL_K").equalTo(ds_with_correct_nums.col("TOTAL_K"))))
                                            ;
        // now we cache (save in memory or storage (if memory is not enough) the dataset since we dont want it to be loaded everytime for each query)
        // caching takes more time due to the write phase (that is hidden from the web UI  and it is considered as computing time)                                  
        ds_corrected.cache();

        // Q1 Number of lethal accidents per week throughout the entire dataset
        
        // We are interested only in lethal accidents
        final Dataset<Row> ds_lethal_accidents = ds_corrected.filter(ds_corrected.col("TOTAL_K").gt(0));

        // Also we need a column YEAR and WEEK that is obtained from DATE
        final Dataset<Row> ds_lethal_accidents_week_year = ds_lethal_accidents.withColumn("WEEK",weekofyear(to_date(ds_lethal_accidents.col("DATE"),"MM/dd/yyyy")))
                															   .withColumn("YEAR", year(to_date(ds_lethal_accidents.col("DATE"),"MM/dd/yyyy")))
                															   ;
        // Following the standard that is used by weekofyear we should consider two special cases for YEAR and WEEK
        // If Week=1 and Month=12 then Year=Year+1
        final Dataset<Row> ds_lethal_accidents_week_year_2 = ds_lethal_accidents_week_year.withColumn("CORRECT_YEAR1",
                                        when(ds_lethal_accidents_week_year.col("WEEK").equalTo(1).and(
                                            month(to_date(ds_lethal_accidents_week_year.col("DATE"),"MM/dd/yyyy")).equalTo(12))
                                        , ds_lethal_accidents_week_year.col("YEAR").plus(1)).otherwise(ds_lethal_accidents_week_year.col("YEAR")));
        // If Week=52 and Month=1 then Year=Year-1
        final Dataset<Row> ds_lethal_accidents_week_year_3 = ds_lethal_accidents_week_year_2.withColumn("CORRECT_YEAR2",
                                        when(ds_lethal_accidents_week_year_2.col("WEEK").geq(52).and(
                                            month(to_date(ds_lethal_accidents_week_year_2.col("DATE"),"MM/dd/yyyy")).equalTo(1))
                                                , ds_lethal_accidents_week_year_2.col("CORRECT_YEAR1").minus(1)).otherwise(ds_lethal_accidents_week_year_2.col("CORRECT_YEAR1")));
      
        // At this point accidents are only lethal and we have columns YEAR and WEEK so now we can group 
        final Dataset<Row> q1 = ds_lethal_accidents_week_year_3.groupBy("CORRECT_YEAR2","WEEK").agg(count("TOTAL_K").as("N. LETHAL ACCIDENTS"))
        														.orderBy("CORRECT_YEAR2", "WEEK")
        														;

        //System.out.println("Query 1:");
        q1.show();
        //q1.write().format("csv").save(filePath+testNumber+"_"+String.valueOf(test_id)+"/query1/");


 
    // Q2 Number  of  accidents  and  percentage  of  number  of  deaths  per  contributing  factor  in  the data set
    // I.e., for each contributing factor, we want to know how many accidents were due to that contributing
    // factor and what percentage of these accidents were also lethal.
    
        
        // Option 1) Explode
        //          - Merge columns C1, C2, C3, C4, C5 in a single column (with distinct values)
        //          - Filter the null values because we want to minimize the amount of rows that will be added through explode
        //          - through explode get 1 row for each element in the array C
        
        
        // the idea is to combine all the contributing factors in each row (through a transformation) and 
        // then create a distinct array in each row, the array may contain null values e.g. [A,],[A,,B],[] or [A,B,]
        final Dataset<Row> df_list_of_C = ds_corrected.select((array_distinct(array("C1","C2","C3","C4","C5"))).as("C"),
                                                                ds_corrected.col("UNIQUE KEY"),ds_corrected.col("TOTAL_K"));
    //  System.out.println("after distinct: "+df_list_of_C.count());
        // before using explode it is a good practice to filter the null values in order to minimize the amount of data that will get added
        // even if explode seems a operation that requires a huge amount of computation, it works better than multiple joins (thanks to the optimizer).
        // However, if the max amount of bytes per partition is high we may end up without HEAP SPACE!
        // TODO: check the performance obtained by reducing the default size of partitions (128 MB)
        
        final Dataset<Row> df_list_of_C_no_null = df_list_of_C.withColumn("C_no_null",array_except(df_list_of_C.col("C"),array(lit(null))));
    //  System.out.println("after filtering null values"+df_list_of_C_no_null.count());         
    
        final Dataset<Row> df_exploded = df_list_of_C_no_null.withColumn("CONTRIBUTING FACTOR",explode(df_list_of_C_no_null.col("C_no_null")));
    //  System.out.println("after filtering explode"+df_exploded.count());          

        // after using explode, the idea is to count the number of accidents per contributing factor and sum the total number of deaths
        final Dataset<Row> q2 = df_exploded.groupBy("CONTRIBUTING FACTOR").agg(count("UNIQUE KEY"),sum("TOTAL_K")); 
        
        // adding a % column
        final Dataset<Row> q2_percentage = q2.withColumn("PERCENTAGE OF DEATHS",concat(bround((q2.col("sum(TOTAL_K)").multiply(100)
                                                                                            .divide(q2.col("count(UNIQUE KEY)"))),2),lit("%")))
                                              .drop("sum(TOTAL_K)")
                                              .withColumnRenamed("count(UNIQUE KEY)", "N. ACCIDENTS")
                                              .orderBy("CONTRIBUTING FACTOR")
                                              ;
      q2_percentage.show();
      //q2_percentage.write().format("csv").save(filePath+testNumber+"_"+String.valueOf(test_id)+"/query2/");
    
    /*
     * the reason why explode is better than multiple self-joins is explained here at min 28:
     * https://www.youtube.com/watch?v=aq23P8r7pTo
     * 
        // Option 2) Self-joins
        //          - Separate columns C1 C2 C3 C4 C5 and 'create' 5 tables: |C1-UNIQUE KEY-TOTAL_K|  |C2-UNIQUE KEY-TOTAL_K| ... 
        //          - Perform 4 self-joins and then group and count by UNIQUE KEY and sum TOTAL_K (maybe, i didn't complete this ... check below)
        //          - However, this cost more than a simple explode since we have many null values that with explode get filtered by using array_except
        
        // TEST WITH MULTIPLE JOINS     
        //  final Dataset<Row> ds_con_1 = ds_corrected.select("C1","UNIQUE KEY","TOTAL_K");//.agg(count("CONTRIBUTING FACTOR VEHICLE 1").as("N. ACCIDENTS"),sum("TOTAL_K"));
        //  final Dataset<Row> ds_con_2 = ds_corrected.select("C2","UNIQUE KEY","TOTAL_K");//.agg(count("CONTRIBUTING FACTOR VEHICLE 2").as("N. ACCIDENTS"),sum("TOTAL_K"));
        //  final Dataset<Row> ds_con_3 = ds_corrected.select("C3","UNIQUE KEY","TOTAL_K");//.agg(count("CONTRIBUTING FACTOR VEHICLE 3").as("N. ACCIDENTS"),sum("TOTAL_K"));
        //  final Dataset<Row> ds_con_4 = ds_corrected.select("C4","UNIQUE KEY","TOTAL_K");//.agg(count("CONTRIBUTING FACTOR VEHICLE 4").as("N. ACCIDENTS"),sum("TOTAL_K"));
        //  final Dataset<Row> ds_con_5 = ds_corrected.select("C5","UNIQUE KEY","TOTAL_K");//.agg(count("CONTRIBUTING FACTOR VEHICLE 5").as("N. ACCIDENTS"),sum("TOTAL_K"));     
        //  final Dataset<Row> ds_con_1_f = ds_con_1.withColumnRenamed("UNIQUE KEY", "U1")
        //                                          .withColumnRenamed("TOTAL_K", "TOT_K1");
        //  final Dataset<Row> ds_con_2_f = ds_con_2.withColumnRenamed("UNIQUE KEY", "U2")
        //                                          .withColumnRenamed("TOTAL_K", "TOT_K2");
        
        //  final Dataset<Row> q2_1 = ds_con_1_f.groupBy("C1", "C2", "U1").sum("TOT_K1");
        //  q2_1.show();
        //  final Dataset<Row> q2_2 = q2_1.groupBy("C1").sum("sum(TOT_K1)");
        //  q2_2.show();
        //  final Dataset<Row> q2 = ds_con_1_f.join(ds_con_2_f,(ds_con_2_f.col("C2").equalTo(ds_con_1_f.col("C1")
        //                             )),"inner");
        //      ds_con_1.show();
        //      ds_con_2.show();
        //  ^ it is not completed, it costs more since i tried a simple program to see if self-joins were creating new copies of the dataset
        // and the answer is yes, it considers a new copy for each join -> not the best solution for real applications
 
        // Option 3) Map-Reduce (non ho seguito distributed systems e col Nesi ovviamente non ne abbiamo parlato)
        //          -Map C1, C2, C3, C4, C5  with key TOT_K (e forse anche UNIQUE KEY per contare accidents) 
        //           so we should get (C1,key)->tot_k, (C2,key)->tot_k, etc for each row
        //          -Reduce by Keys -> so if we have (aC1,1234)->1, (aC1,1234)->1  ... due to a double aC1 in a row (that should be counted only once)
        //           we should reduce the key in each map column
        //          I'm not sure how much it would cost but i believe that reducing implies some sort of shuffling even if we are just dealing
        //          with a single row, the idea is similar to the usage of a dictionary that do not maintain the constraint about no duplicate keys
        //          but we remove them by reducingByKey, however this may increase a lot the dataset size, more than explode. Experts suggests that we should 
        //          trust the optimizer and use the functions already present in the high level API. (I know it sounds weird but I watched many videos about spark and they kept repeating it)
        
    
        // Altre opzioni
        //      4) salvarmi i 49 valori distinti che ci sono in ogni contributing factor e mettermi a contarli (come diceva il braga a lezione pero attraverso un accumulator (esiste ma l'abbiamo solo accennato a lezione)))
        //      5) vari self join (diverso dal 2, non mi ricordo esattamente come)
        //      6) add 49 columns (for each distinct contributing factor) and increase the counter (maybe through a User defined aggregate function?)
        //      7) non mi ricordo l'ultimo .... forse era cosi: definisce una classe user defined aggregate function 
        //         e salvati i contatori... una specie di groupby count5C ... https://spark.apache.org/docs/latest/api/java/index.html
        //      7 molto simile a 6 solo che nel 6 si lavora sul dataframe e nel 7 su 49 variabili (volendo un container map<>) nella classe user defined aggregate function 
    */
        
        
    // Q3 Number of accidents and average number of lethal accidents per week per borough.
    //  I.e.,  for  each  borough,  we  want  to  know how  many  accidents  there  were  in that borough each  week,
    //  as  well  as  the  average  number  of  lethal  accidents  that  the borough had per week.
        
        // It may seem tempting to use the dataframe of the first query and then groupBy BOROUGH, however it must be considered that
        // in the first query it was not necessary to count the number of accidents and a good practice is to filter dataframes before using groupBy.
        // Also caching implies writing in memory and it takes time, in the first query after the filter (to consider only lethal accidents)
        // we get a very low number of rows so it makes query 1 to be completed really fast, using groupBy without the filter would increase a lot the computing time.
        // if you print the number of rows before applying the filter it will be 948198
        // and after applying the filter it will be 1076.
        // For query 3 we need the number of accidents so we should redo the operations performed before by using ds_corrected and not
        // a cached version of ds_lethal_accidents_per_week or ds_lethal_accidents.

        final Dataset<Row> ds_borough_no_null = ds_corrected.filter(ds_corrected.col("BOROUGH").isNotNull());
        final Dataset<Row> ds_w_n_y = ds_borough_no_null.withColumn("WEEK",weekofyear(to_date(ds_borough_no_null.col("DATE"),"MM/dd/yyyy")))
                .withColumn("YEAR", year(to_date(ds_borough_no_null.col("DATE"),"MM/dd/yyyy")))
                ;
        final Dataset<Row> ds_w_n_y_2 = ds_w_n_y.withColumn("CORRECT_YEAR1",
        								when(ds_w_n_y.col("WEEK").equalTo(1).and(
        									month(to_date(ds_w_n_y.col("DATE"),"MM/dd/yyyy")).equalTo(12))
        										,ds_w_n_y.col("YEAR").plus(1))
        								.otherwise(ds_w_n_y.col("YEAR")));
        final Dataset<Row> ds_w_n_y_3 = ds_w_n_y_2.withColumn("CORRECT_YEAR2",
										when(ds_w_n_y_2.col("WEEK").geq(52).and(
											month(to_date(ds_w_n_y_2.col("DATE"),"MM/dd/yyyy")).equalTo(1))
												,ds_w_n_y_2.col("CORRECT_YEAR1").minus(1))
										.otherwise(ds_w_n_y_2.col("CORRECT_YEAR1")));
        
        final Dataset<Row> q3_with_lethal_column= ds_w_n_y_3.withColumn("LETHAL?",when(ds_w_n_y_3.col("TOTAL_K").gt(0), 1)
                                                                                            .otherwise(0));
        /* TODO: ora che ho modificato YEAR ricalcola i risultati  
         * cosi per BRONX viene Num. accidents: 91180 e Num lethal: 107 -> non so come hai trovato 46% :thinking:
        
        final Dataset<Row> q3 = q3_with_lethal_column.groupBy("BOROUGH")
                                                 .agg(count("UNIQUE KEY").as("N. Accidents"),
                                                     sum("LETHAL?").as("sum(Lethal)"))
                                                 .orderBy("BOROUGH")
                                                ;
         */
        
        // in questo modo invece viene BRONX, year, week, number of accidents in that week, avg(lethal accidents)
        // for example if we have a 5 accidents in a given week and 3 were lethal the average is 3/5 
        // in general there are many weeks that did no have any lethal accident so we have many avg(lethal) 0
        final Dataset<Row> q3 = q3_with_lethal_column.groupBy("BOROUGH","CORRECT_YEAR2","WEEK")
                                                     .agg(count("UNIQUE KEY").as("N. ACCIDENTS"),
                                                         avg("LETHAL?").as("avg(Lethal)"))
                                                     .orderBy("BOROUGH","CORRECT_YEAR2","WEEK")
                                                    ;
        // avg returns many floating values so we will round them to the 3rd number
        final Dataset<Row> q3_2 = q3.withColumn("avg(LETHAL)",bround(q3.col("avg(Lethal)"),3));
        q3_2.show();
        //q3_2.write().format("csv").save(filePath+testNumber+"_"+String.valueOf(test_id)+"/query3/");
        //Scanner a = new Scanner(System.in); a.nextLine();
        // Optional for .cache() or persist(). It should be called anyway just before the SparkContext gets closed
        ds_corrected.unpersist();
        
    }
}

