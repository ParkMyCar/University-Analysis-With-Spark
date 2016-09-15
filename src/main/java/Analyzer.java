/** Parker Timmerman September 8th 2016
 *  --University Analysis with Spark--
 *  
 *  Take the data from the DataManager and perform analysis on it
 */

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import java.util.*;
import java.io.*;

public class Analyzer 
{
	public static Hashtable<String, Integer> headerMap = new Hashtable<String, Integer>();
	private static String schoolName = "";
	private static String schoolStatistic = "";
	
	public Analyzer()
	{
		buildHeader();
	}
	
	public static List<String[]> analyze(String[] args)
	{
		JavaRDD<String> data = DataManager.getDataManager().getData();
		JavaRDD<String[]> schoolsArray = data.map(new SplitLines());

 		List<String[]> retVal = new ArrayList<String[]>();
		
		int funcType = Integer.parseInt(args[0]);
		
		switch (funcType) {
			case 1: //Search for school
				schoolName = args[1];
				JavaRDD<String[]> returnedSchools = schoolsArray.filter(new FindSchool()); 
				return returnedSchools.collect();
				
			case 2: //Top - 25 search
				schoolStatistic = args[1];
				JavaPairRDD<Float, String> pairs = schoolsArray.mapToPair(new SchoolPairStatistic()).sortByKey(false);
				List<Tuple2<Float, String>> top25 = pairs.take(50);
				List<String[]> top25StringList = new ArrayList<String[]>();
				for(Tuple2<Float,String> pair : top25)
				{
					String[] temp = { String.valueOf(pair._1), pair._2 };
					top25StringList.add(temp);
				}
				return top25StringList;
			case 3: //Trend over time
				schoolName = args[1];
				schoolStatistic = args[2];
				JavaRDD<String> legacyData = DataManager.getDataManager().getLegacyData();
				JavaPairRDD<Float, String> legacyArray = legacyData.map(new SplitLines()).filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
				JavaPairRDD<Float, String> currentArray = schoolsArray.filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
 				
				List<Tuple2<Float, String>> legacyTupleList = legacyArray.collect();
				List<Tuple2<Float, String>> currentTupleList = currentArray.collect();
				
				List<String[]> analyzedDataList = new ArrayList<String[]>();
				for(int i = 0; i < currentTupleList.size(); i++)
				{
					String[] temp = { String.valueOf(currentTupleList.get(i)._1), currentTupleList.get(i)._2 };
					String[] temp2 = { String.valueOf(legacyTupleList.get(i)._1), legacyTupleList.get(i)._2 };
					analyzedDataList.add(temp);
					analyzedDataList.add(temp2);
				}
				return analyzedDataList;
			default:
				break;
		}
		return retVal;
	}
	
	//Functions
	
	//Takes each line and turns it into an array so we can find values easily
    static class SplitLines implements Function<String, String[]>
    {
    	static final long serialVersionUID = 1L;
    	public String[] call(String str) 
    	{ 
    		return str.split(","); 
    	}
    }
    
    //Searches for schools based on the 4th element in the string array which is (should be) the school name 
    static class FindSchool implements Function<String[], Boolean>
    {
    	static final long serialVersionUID = 2L;
    	public Boolean call(String[] schoolArgs)
    	{ 
    		return schoolArgs[3].contains(schoolName); 
    	}
    }
    
    //Takes a school name and pairs it with the statistic we are searching for
    static class SchoolPairStatistic implements PairFunction<String[], Float, String>
    {
    	static final long serialVersionUID = 3L;
    	public Tuple2<Float, String> call(String[] args)
    	{
    		int index = headerMap.get(schoolStatistic);
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
