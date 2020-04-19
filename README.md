
1. Clone the project from Github

git clone git@github.com:muhammadrezaulkarim/FileUtilityApacheSpark.git

2. Build the project in Windows command prompt or Linux terminal with Apache Maven from the application root directory:

mvn -U clean package

3. Copy all necessary files (app-config.json and customlog4j.properties) from the 'src\main\resources' directory to the 'target' directory.

4. Edit the app-config.json file and specify the test file location (absolute path of the directory containing the test file) and test 

file name. Data directory has several test files. 'log.json' is the default test file name.

5. Move to the 'target' directory in Windows command prompt or Linux terminal and execute the application:

java -jar solutions-0.0.1-SNAPSHOT-jar-with-dependencies.jar local

The argument 'local' is required to run the program in local desktop/laptop without the need for setting up Apache Spark Cluster.


*** Please note that Java 8, Apache Maven, Apache Spark is a pre-requisite for this project. 

Help regarding setting up Apache Spark cluster and running in a Spark cluster can be found in the readme file of the following project:

https://github.com/muhammadrezaulkarim/datamerging
