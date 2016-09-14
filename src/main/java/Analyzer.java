/** Parker Timmerman September 8th 2016
 *  --University Analysis with Spark--
 *  
 *  Take the data from the DataManager and perform analysis on it
 */

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import java.util.*;
import java.io.*;

public class Analyzer 
{
	public static Hashtable<String, Integer> headerMap = new Hashtable<String, Integer>();
	static private String funcCriteria = "";
	
	public Analyzer()
	{
		buildHeader();
	}
	
	public List<String[]> analyze(String[] args)
	{
		JavaRDD<String> data = DataManager.getDataManager().getData();
 		JavaRDD<String[]> outputData = null;
 		List<String[]> retVal = new ArrayList<String[]>();
		
		int funcType = Integer.parseInt(args[0]);
		funcCriteria = args[1];
		
		switch (funcType) {
			case 1: //Search for school
				JavaRDD<String> returnedSchools = data.filter(str -> str.contains(funcCriteria));
				outputData = returnedSchools.map(new SplitLines());
				return outputData.collect();
				
			case 2: //Top - 25 search
				JavaRDD<String[]> lines = data.map(new SplitLines());
				JavaPairRDD<Float, String> pairs = lines.mapToPair(new SchoolPairStatistic());
				JavaPairRDD<Float, String> sortedPairs = pairs.sortByKey(false);
				List<Tuple2<Float, String>> top25 = sortedPairs.take(50);
				List<String[]> top25StringList = new ArrayList<String[]>();
				for(Tuple2<Float,String> pair : top25)
				{
					String[] temp = { String.valueOf(pair._1), pair._2 };
					top25StringList.add(temp);
				}
				return top25StringList;
				
			default:
				break;
		}
		return retVal;
	}
	
	//Functions
    static class SplitLines implements Function<String, String[]>
    {
    	public String[] call(String str) { return str.split(","); }
    }
    
    static class SchoolPairStatistic implements PairFunction<String[], Float, String>
    {
    	public Tuple2<Float, String> call(String[] args)
    	{
    		int index = headerMap.get(funcCriteria);
    		if (args[index].equals("NULL"))
    		{
    			return new Tuple2<Float, String>(-1f, args[3]);
    		}
    		return new Tuple2<Float, String>(Float.parseFloat(args[index]), args[3]);
    	}
    }
	
	//Private Methods
	private void buildHeader()
    {
		String filePath = new File("").getAbsolutePath();
    	String headerFile = filePath.concat("/src/main/resources/university_header.csv");
    	String line = "";
        String csvSplitBy = ",";
        String[] header = new String[50];
    	try
    	{
    		BufferedReader br = new BufferedReader(new FileReader(headerFile));
    		while ((line = br.readLine()) != null)
    		{
    			header = line.split(csvSplitBy);
    		}
    		br.close();
    	}
    	catch (FileNotFoundException e)
    	{
    		System.out.println("File not found!");
    	}
    	catch (IOException e)
    	{
    		System.out.println("Input/Output error! Please make sure the file exists, restart the program, and try again!");
    	}
    	
    	for (int i = 0; i < header.length; i++)
    	{
    		headerMap.put(header[i], i);
    	}
    }
}
