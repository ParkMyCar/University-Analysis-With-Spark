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
	//Private Member Variables
	private static DataManager instance;
	private JavaRDD<String> data;
	
	SparkConf conf = new SparkConf()
		.setAppName("UniversityAnalysis")
		.setMaster("local");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	//Private Methods
	private DataManager()
	{
		loadData();
	}
	
	private void loadData()
	{
		//Keep the most current data separate because that is all we need for searching for a school and top 50. Not loading the entire data set will make these operations faster
		String filePath = new File("").getAbsolutePath();
		String pathToData = filePath.concat("/src/main/resources/MERGED2013_PP.csv");
		sc.setLogLevel("WARN");
		data = sc.textFile(pathToData);
		data.cache();
		
		//Had to initialize the legacy data with something so 2012 it was first
		String twoTwelveFilePath = new File("").getAbsolutePath();
		String twoTwelvePathToData = twoTwelveFilePath.concat("/src/main/resources/MERGED2012_PP.csv");
		JavaRDD<String> legacyData = sc.textFile(twoTwelvePathToData);
				
		//load the rest of the data sets
		String legacyFilePath = new File("").getAbsolutePath();
		for (int i = 2011; i >= 1996; i--)
		{
			String legacyPathToFile = legacyFilePath + "/src/main/resources/MERGED" + String.valueOf(i) + "_PP.csv";
			JavaRDD<String> temp = sc.textFile(legacyPathToFile);
			legacyData = legacyData.union(temp);
		}
	}
	
	private JavaRDD<String> loadLegacyData()
	{
		String twoTwelveFilePath = new File("").getAbsolutePath();
		String twoTwelvePathToData = twoTwelveFilePath.concat("/src/main/resources/MERGED2012_PP.csv");
		JavaRDD<String> legacyData = sc.textFile(twoTwelvePathToData);
				
		String legacyFilePath = new File("").getAbsolutePath();
		for (int i = 2011; i >= 1996; i--)
		{
			String legacyPathToFile = legacyFilePath + "/src/main/resources/MERGED" + String.valueOf(i) + "_PP.csv";
			JavaRDD<String> temp = sc.textFile(legacyPathToFile);
			legacyData = legacyData.union(temp);
		}
		
		return legacyData;
		//Note: would have liked to cache the RDD to make future computations faster but on my machine I run into a memory error
	}
	
	//Public Methods
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
		return loadLegacyData();
	}
	
	public void cleanUp()
	{
		sc.close();
	}
}
