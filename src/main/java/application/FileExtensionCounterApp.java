package application;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.Gson;


import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.when;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Apache Spark Best practices: https://docs.microsoft.com/en-us/azure/databricks/spark/latest/rdd-streaming/developing-streaming-applications
 */

public class FileExtensionCounterApp implements Serializable {

	private static final long serialVersionUID = 1L;

	private static Logger logger = LoggerFactory.getLogger(FileExtensionCounterApp.class);


	private String intputFileAbolutePath;
	private String inputDirectoryName, inputFileName;

	Dataset<Row> dfJSON, dfIntermediate, dfFileExtensionAggregate;

	SparkSession spark = null;

	/**
	 * Load application configuration JSON file
	 */

	public void loadApplicationConfig(String configFileName) throws FileNotFoundException {
				
		Gson gson = new Gson();

		// convert JSON file to map
		Map<?, ?> configMap = gson.fromJson(new FileReader(configFileName), Map.class);

		inputDirectoryName = (String) configMap.get("file-location");
		inputFileName = (String) configMap.get("file-name");

		intputFileAbolutePath = inputDirectoryName + System.getProperty("file.separator") + inputFileName;

	}

	/**
	 * Initialize environment (e.g. Spark Session)
	 */

	public void initializeEnvironment(String[] args) {

		// Run on local machine when local is specified as a program argument
		if (args.length > 0 && args[0].equals("local")) {

			spark = SparkSession.builder().appName("File Extension Counter").master("local[10]")
					.config("spark.sql.session.timeZone", "UTC")
					.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:customlog4j.properties -Dlog4j.debug=true")
					.config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:customlog4j.properties -Dlog4j.debug=true")
					.getOrCreate();
		} else // (master not specified) when deployed in a cluster with spark-submit command
		{
			spark = SparkSession.builder().appName("File Extension Counter").config("spark.sql.session.timeZone", "UTC")
					.config("spark.sql.session.timeZone", "UTC")
					.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:customlog4j.properties -Dlog4j.debug=true")
					.config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:customlog4j.properties -Dlog4j.debug=true")																								
					.getOrCreate();
			//spark.sparkContext().setLogLevel("INFO");
		}

	}

	/**
	 * Method for reading input JSON file in a dataframe
	 */
	public void readInputFile() {

		// Reads the JSON file in a data frame
		dfJSON = spark.read().format("json").option("multiline", false).load(intputFileAbolutePath);
		dfJSON.cache();

	}

	/**
	 * Transform dataframe and create intermdediate columns
	 */
	public void transformData() {

		// Split the file name column based on dot. create a temporary column named filenamearray
		dfIntermediate = dfJSON.withColumn("filenamearray", split(dfJSON.col("nm"), "\\."));

		// Separate file name and file extension from the given file name. Drop the temporary column
		dfIntermediate = dfIntermediate
				.withColumn("filenamewithoutextension", dfIntermediate.col("filenamearray").getItem(0))
				.withColumn("fileextension", dfIntermediate.col("filenamearray").getItem(1)).drop("filenamearray");

		// Remove records where fileextension or filenamewithoutextension is null (not used)
		// dfIntermediate = dfIntermediate.filter("filenamewithoutextension is not null and fileextension is not null");

		// consider the no file extension as well instead of removing them (used)
		// reset the filenamewithoutextension and fileextension columns
		dfIntermediate = dfIntermediate.withColumn("filenamewithoutextension",
				when(dfIntermediate.col("filenamewithoutextension").equalTo("null"), dfIntermediate.col("nm"))
						.otherwise(dfIntermediate.col("filenamewithoutextension")));
		dfIntermediate = dfIntermediate.withColumn("fileextension",
				when(dfIntermediate.col("fileextension").isNull(), "noextension")
						.otherwise(dfIntermediate.col("fileextension")));
		dfIntermediate.cache();

		// Detect duplicate rows based on the columns fileextension and filenamewithoutextension
		dfIntermediate = dfIntermediate.dropDuplicates("fileextension", "filenamewithoutextension");
		dfIntermediate.cache();

	}

	/**
	 * Compute file extension count using aggregate and store in a separate dataframe
	 */

	public void computeExtensionCount() {

		dfFileExtensionAggregate = dfIntermediate.groupBy(dfIntermediate.col("fileextension"))
				.agg(count("fileextension")).orderBy("fileextension");
		dfFileExtensionAggregate = dfFileExtensionAggregate.withColumnRenamed("count(fileextension)", "count");
		dfFileExtensionAggregate.cache();

	}

	/**
	 * print out results partition-by-partition without flooding driver JVM
	 */
	public void printExtensionSummary() {
		// When writing anonymous inner class, named inner class or lambda, Java creates reference to the outer class in the inner class.
		// So even if the inner class is serializable, the exception (org.apache.spark.SparkException: Task not serializable) can occur.
		// The outer class must be also serializable. Add implements Serializable to outer class FileExtensionCounterApp
		/*
		 * 
		 * dfFileExtensionAggregate.foreachPartition(new ForeachPartitionFunction<Row>()
		 * { public void call(Iterator<Row> t) throws Exception { while (t.hasNext()) {
		 * Row row = t.next(); logger.info(row.getString(0) + ": " + row.getLong(1)); }
		 * } });
		 */

		// The first column of the dataframe contains the extension name, the column contaisn the extension count
		dfFileExtensionAggregate.foreachPartition(partition -> {
			while (partition.hasNext()) {
				Row row = partition.next();
				logger.info(row.getString(0) + ": " + row.getLong(1) );
			}

		});
		 
	}

	/**
	 * write the output in a CSV file
	 */
	public void writeOutPutFile() {

		dfFileExtensionAggregate = dfFileExtensionAggregate.repartition(1);
		// Save the output file
		dfFileExtensionAggregate.write().format("csv").option("header", false).mode(SaveMode.Overwrite)
				.save(inputDirectoryName + System.getProperty("file.separator") + "output.csv");

	}

}
