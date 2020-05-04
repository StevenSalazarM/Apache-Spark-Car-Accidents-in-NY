# Car Accidents in New York
The goal of this project is to infer qualitative data regarding the car accidents in New York City. In particular, it is asked to perform the following queries:
### Query 1
Number of lethal accidents per week throughout the entire dataset. 
### Query 2
Number  of  accidents  and  percentage  of  number  of  deaths  per  contributing  factor  in  the dataset.
> for each contributing factor, we want to know how many accidents were due to that  contributing factor and what percentage of these accidents were also lethal.

### Query 3
Number of accidents and average number of lethal accidents per week per borough.
> for  each  borough,  we  want  to  know how  many  accidents  there  were  in that borough each  week,  as  well  as  the  average  number  of  lethal  accidents  that  the borough had per week.

## Solution
The dataset that is used to perform the three queries is available at [NYPD_Motor_Vehicle_Collisions](http://ssmgames.altervista.org/NYPD_Motor_Vehicle_Collisions.csv).
In order to complete the queries requested it was considered that:
- In the dataset some rows contain incorrect values since:

 `# Persons Injured = # Cyclist Inj + # Pedestrians Inj + # Motorist Inj `
 and 
 `# Persons Killed = # Cyclist Kill + # Pedestrians Kill + # Motorist Kill `

- From the dataset it is possible to see that in its structure it was not considered to have YEAR and WEEK as direct data, so it these two information had to be calculated from DATE.
- There are 5 columns with the same domain called CONTRIBUTING FACTOR X that is be merged into a single array column.

### Usage

#### Local Mode
1. Clone the project.

2. Download the dataset.

3. Move the dataset into the *files* directory of the project.

4. Open the project with Eclipse or any IDE that  supports maven.

5. Run as Java Application without passing any parameter.

#### Cluster Mode
1. Download Spark 2.4+ and configure it as you like.

2. Start Spark and make sure that you connect to at least one worker.

3. Clone the project.

4. Download the dataset.

5. Move the dataset into the *files* directory of the project.

6. Compile the project with maven

 > mvn package

6. Submit the project through spark-submit, for example:
> spark-submit --class it.polimi.middleware.spark.car.accidents.CarAccidentsCache car_accidents.jar spark://master_ip:port dataset_directory/ test_number 
