/** Parker Timmerman September 8th 2016
 *  --University Analysis with Spark--
 *  
 *  Load the CSV files with university data
 */

import java.io.File;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class DataManager 
{
	private static DataManager instance;
	private JavaRDD<String> data;
	private JavaRDD<String> legacyData;
	
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
		
		String legacyFilePath = new File("").getAbsolutePath();
		String legacyPathToData = legacyFilePath.concat("/src/main/resources/MERGED2012_PP.csv");
		legacyData = sc.textFile(legacyPathToData);
		legacyData.cache();
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
	
	public JavaRDD<String> getLegacyData()
	{
		return legacyData;
	}
	
	public void cleanUp()
	{
		sc.close();
	}
}
