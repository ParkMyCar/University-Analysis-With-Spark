/** Parker Timmerman September 8th 2016
 *  --University Analysis with Spark--
 *  
 *  Load the CSV files with university data
 */

import java.io.File;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;


public class DataManager 
{
	private static DataManager instance;
	private JavaRDD<String> data;
	
	SparkConf conf = new SparkConf()
		.setAppName("UniversityAnalysis")
		.setMaster("local");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	private DataManager()
	{
		loadData();
	}
	
	private void loadData()
	{
		String filePath = new File("").getAbsolutePath();
		String pathToData = filePath.concat("/src/main/resources/MERGED2013_PP.csv");
		sc.setLogLevel("WARN");
		data = sc.textFile(pathToData);
		data.cache();
	}
	
	public static DataManager getDataManager()
	{
		if (instance == null)
		{
			instance = new DataManager();
		}
		return instance;
	}
	
	public JavaRDD<String> getData()
	{
		return data;
	}
	
	public void cleanUp()
	{
		sc.close();
	}
}
