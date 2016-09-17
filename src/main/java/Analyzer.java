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
	//Private Member Variables
	public static Hashtable<String, Integer> headerMap = new Hashtable<String, Integer>();
	private static String schoolName = "";
	private static String schoolStatistic = "";
	
	//Public Methods
	public Analyzer()
	{
		buildHeader();
	}
	
	public static List<String[]> analyze(String[] args)
	{
		JavaRDD<String> data = DataManager.getDataManager().getData();
		JavaRDD<String[]> schoolsArray = data.map(new SplitLines());
		
		JavaRDD<String> legacyData;
		
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
				legacyData = DataManager.getDataManager().getLegacyData();
				JavaPairRDD<Float, String> legacyArray = legacyData.map(new SplitLines()).filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
				JavaPairRDD<Float, String> currentArray = schoolsArray.filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
 				
				JavaPairRDD<Float, String> allData = currentArray.union(legacyArray);
				List<Tuple2<Float, String>> allTupleList = allData.collect();
				
				List<String[]> analyzedDataList = new ArrayList<String[]>(); 
				for (Tuple2<Float, String> tuple : allTupleList)
				{
					String[] temp = { String.valueOf(tuple._1), tuple._2 };
					analyzedDataList.add(temp);
				}
				
				return analyzedDataList;
				
			case 4: //Correlation between two statistics
				schoolName = args[1];
				schoolStatistic = args[2];
				
				legacyData = DataManager.getDataManager().getLegacyData();
				
				//SO = Statistic One or the first statistic
				JavaPairRDD<Float, String> legacyArraySO = legacyData.map(new SplitLines()).filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
				JavaPairRDD<Float, String> currentArraySO = schoolsArray.filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
				
				JavaPairRDD<Float, String> allDataSO = currentArraySO.union(legacyArraySO);
				List<Tuple2<Float, String>> allTupleListSO = allDataSO.collect();
				
				//ST = Statistic Two or the second statistic <TODO> Find a way to pass a variable to a function, that way I don't need to use member variables as a work around, should be simple (I would think)
				schoolStatistic = args[3]; //The SchoolPairStatistic function uses the schoolStatistic member variable, so we must set the variable to the second statistic
				JavaPairRDD<Float, String> legacyArrayST = legacyData.map(new SplitLines()).filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
				JavaPairRDD<Float, String> currentArrayST = schoolsArray.filter(new FindSchool()).mapToPair(new SchoolPairStatistic());
				
				JavaPairRDD<Float, String> allDataST = currentArrayST.union(legacyArrayST);
				List<Tuple2<Float, String>> allTupleListST = allDataST.collect();
				
				int listLength;
				if(allTupleListSO.size() != allTupleListST.size())
				{ listLength = Math.min(allTupleListSO.size(), allTupleListST.size()); }
				else
				{ listLength = allTupleListSO.size(); }
				
				List<String[]> correlatedDataList = new ArrayList<String[]>(listLength);
				
				List<Float> so = new ArrayList<Float>();
				List<Float> st = new ArrayList<Float>();
				
				for (int i = 0; i < listLength; i++) //Some statistics are missing values for certain years so we can't use those for correlation
				{
					if (allTupleListSO.get(i)._1 != -1.0 && allTupleListST.get(i)._1 != -1.0)
					{
						so.add(allTupleListSO.get(i)._1);
						st.add(allTupleListST.get(i)._1);
					}
				}
				
				String[] schoolNameAndCorrelation = { schoolName, String.valueOf(correlationCoefficient(so, st)), args[2], args[3] }; //school name, correlation value, first statistic, second statistic
				correlatedDataList.add(schoolNameAndCorrelation);
				
				for (int i = 0; i < listLength; i++)
				{
					String[] temp = { allTupleListSO.get(i)._2, String.valueOf(allTupleListSO.get(i)._1), String.valueOf(allTupleListST.get(i)._1) };
					correlatedDataList.add(temp);
				}
				
				return correlatedDataList;
			default:
				break;
		}
		return retVal;
	}
	
	
	//Private Methods	
	private static double correlationCoefficient(List<Float> xs, List<Float> ys)
	{
		double sx = 0.0;
	    double sy = 0.0;
	    double sxx = 0.0;
	    double syy = 0.0;
	    double sxy = 0.0;

	    int n = xs.size();

	    for(int i = 0; i < n; ++i) {
	      double x = xs.get(i);
	      double y = ys.get(i);

	      sx += x;
	      sy += y;
	      sxx += x * x;
	      syy += y * y;
	      sxy += x * y;
	    }

	    double cov = sxy / n - sx * sy / n / n;
	    double sigmax = Math.sqrt(sxx / n -  sx * sx / n / n);
	    double sigmay = Math.sqrt(syy / n -  sy * sy / n / n);

	    return cov / sigmax / sigmay;
	}
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
    		return schoolArgs[3].toUpperCase().contains(schoolName.toUpperCase()); 
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
}
