import java.util.*;
import scala.Tuple2;

public class UserInterface 
{
	Analyzer analyzer = new Analyzer();
	
	//Private Member Variables
	private List<Tuple2<String, String>> keys = new ArrayList<Tuple2<String, String>>();
	
	//Constructor
	public UserInterface()
	{
		/*<TODO> At a later date. Add a settings file that will contain settings or preferences
		 * ie. Location, Max Tuition, School Size, Major, could help in a search
		 */
		DataManager.getDataManager();
		String title = "╦ ╦┌┐┌┬┬  ┬┌─┐┬─┐┌─┐┬┌┬┐┬ ┬  ╔═╗┌┐┌┌─┐┬ ┬ ┬┌─┐┬┌─┐\n"
					 + "║ ║││││└┐┌┘├┤ ├┬┘└─┐│ │ └┬┘  ╠═╣│││├─┤│ └┬┘└─┐│└─┐\n"
					  +"╚═╝┘└┘┴ └┘ └─┘┴└─└─┘┴ ┴  ┴   ╩ ╩┘└┘┴ ┴┴─┘┴ └─┘┴└─┘";
		System.out.println(title + "\n");
		System.out.print("Welcome! Please pick one of the options below: \n"
				+ "\t1: Seach for a school\n"
				+ "\t2: Top - 50 search based on a statistic\n"
				+ "\t3: View trend over time\n"
				+ "\t4: Correlation between two statistics\n"
				+ "\t5: Exit\n\n");
		
		fillKeysList();
	}
	
	//Public Methods
	public void getInput()
	{
		Scanner kbd = new Scanner(System.in);
		int function = 0;
		String stat = null;
		String stat2 = null;
		boolean analyze = true;
		String[] args = new String[4];
		while(function != 5) 
		{
			System.out.print("Selection (number): ");
			if (kbd.hasNextInt()) //Verifies the input is an integer
			{ 
				function = kbd.nextInt();
			}
			kbd.nextLine();
			
			analyze = true;
			
			switch (function) {
			case 1: //Search for school
				System.out.print("School to search for: ");
				args[1] = kbd.nextLine();
				break;
				
			case 2: //Top - 10 Search
				listStatistics();
				System.out.print("Top 50 schools in: ");
				stat = kbd.nextLine();
				if (interpretInput(stat)._1)
					args[1] = interpretInput(stat)._2;
				else
					analyze = false;
				
				break;
			case 3: //Trend over time
				listStatistics();
				System.out.print("School to analyze : ");
				args[1] = kbd.nextLine(); //School
				
				System.out.print("Statistic to analyze: ");
				stat = kbd.nextLine(); //Statistic
				if (interpretInput(stat)._1)
					args[2] = interpretInput(stat)._2;
				else
					analyze = false;
				
				break;
				
			case 4: //Correlation between two statistics
				listStatistics();
				System.out.print("School to analyze: ");
				args[1] = kbd.nextLine(); //School
				
				System.out.print("First statistic: ");
				stat = kbd.nextLine(); //First statistic for correlation
				if (interpretInput(stat)._1)
					args[2] = interpretInput(stat)._2;
				else
					analyze = false;
				
				System.out.print("Second statistic: ");
				stat2 = kbd.nextLine(); //Second statistic for correlation
				if (interpretInput(stat2)._1)
					args[3] = interpretInput(stat2)._2;
				else
					analyze = false;
				
				break;
				
			case 5: //Exit
				function = 5;
				break;
			default:
				System.out.println("Command not recognized! Please enter the number of the function you would like to perform.\n");
				break;
			}
			args[0] = String.valueOf(function);
			
			if (analyze)
				handleOutput(analyzer.analyze(args), function);
			
			else
				System.out.println("There was invalid input, please try again!");
		}
		kbd.close();
		DataManager.getDataManager().cleanUp();
	}
	
	//Private Methods
	private void handleOutput(List<String[]> results, int function)
	{
		switch (function)
		{
			case 1: //School search
				int i = 1;
				for(String[] school : results)
				{
					System.out.println(i++ + ". " + school[3]
							+ "\n\t Location: " + school[analyzer.headerMap.get("CITY")] + ", " + school[analyzer.headerMap.get("STABBR")]
							+ "\n\t Undergrad Students: " + school[analyzer.headerMap.get("UGDS")]
							+ "\n\t Admission Rate: " + school[analyzer.headerMap.get("ADM_RATE")]
							+ "\n\t Average Faculty Salary (Monthly): " + school[analyzer.headerMap.get("AVGFACSAL")]
							+ "\n\t Tuition:"
							+ "\n\t\t In-State: $" + school[analyzer.headerMap.get("TUITIONFEE_IN")]
							+ "\n\t\t Out of State: $" + school[analyzer.headerMap.get("TUITIONFEE_OUT")] + "\n");
				}
				break;
			
			case 2: //Top-50 Search
				int j = 1;
				for(String[] pair : results)
				{
					if (Float.parseFloat(pair[0]) <= 1.0)
					{
						System.out.println(j++ + ". " + pair[1] + ": " + (Float.parseFloat(pair[0]) * 100) + "%");
					}
					else
					{
						System.out.println(j++ + ". " + pair[1] + ": " + pair[0]);
					}
				}
				break;
			case 3: //Trend over time
				int k = 2013;
				for(String[] pair : results)
				{
					float value = Float.parseFloat(pair[0]);
					if (value == -1.0)
					{
						System.out.println(k-- + ": N/A\t" + pair[1]);
					}
					else
					{
						System.out.println(k-- + ": " + value + "\t" + pair[1]);
					}
				}
				break;
			case 4: //Correlated Statistics
				int l = 2013;
				String schoolName = results.get(0)[0];
				double correlationValue = Double.parseDouble(results.get(0)[1]);
				String firstStat = results.get(0)[2];
				String secondStat = results.get(0)[3];
				System.out.println(schoolName + "\nCorrelation between " + firstStat + " and " + secondStat + ": " + correlationValue);
				for (int m = 1; m < results.size(); m++)
				{
					String[] pair = results.get(m);
					float value1 = Float.parseFloat(pair[1]);
					float value2 = Float.parseFloat(pair[2]);
					String str1 = pair[1];
					String str2 = pair[2];
					
					if (value1 == -1.0)
					{ str1 = "N/A"; }
					if (value2 == -1.0)
					{ str2 = "N/A"; }
					
					System.out.println(l-- + ":\t" + str1 + "\t\t" + str2);
				}
				break;
			default:
				break;
		}
	}

	private Tuple2<Boolean, String> interpretInput(String str)
	{
		if (tryParse(str))
		{
			int index = Integer.parseInt(str);
			if (index > 0 && index < keys.size())
			{
				return new Tuple2<Boolean, String>(true, keys.get(index-1)._2);
			}
		}
		else if (!tryParse(str))
		{
			for (Tuple2<String, String> pair : keys)
			{
				if (str.equalsIgnoreCase(pair._1))
				{
					return new Tuple2<Boolean, String>(true, pair._2);
				}
			}
		}
		return new Tuple2<Boolean, String>(false, "Invalid input, please try again");
	}
	
	private void listStatistics()
	{		
		System.out.println("\nSTATISTICS: ");
		
		for (int i = 0; i < keys.size(); i++)
		{
			if (i != 0 && i%3 == 0)
			{
				System.out.printf("%-65.65s", "\n" + (i+1) + ". " + keys.get(i)._1);
			}
			else
			{
				System.out.printf("%-65.65s", (i+1) + ". " + keys.get(i)._1);
			}	
		}
		System.out.println("\n\nSelect the statistic by typing the name or using the number!\n");
		
	}
	
	private void fillKeysList()
	{
		keys.add(new Tuple2<String, String>("Admission Rate", "ADM_RATE"));
		keys.add(new Tuple2<String, String>("25th Percentile of Reading SAT Scores", "SATVR25"));
		keys.add(new Tuple2<String, String>("75th Percentile of Reading SAT Scores", "SATVR75"));
		keys.add(new Tuple2<String, String>("25th Percentile of Math SAT Scores", "SATMT25"));
		keys.add(new Tuple2<String, String>("75th Percentile of Math SAT Scores", "SATMT75"));
		keys.add(new Tuple2<String, String>("25th Percentile of Writing SAT Scores", "SATWR25"));
		keys.add(new Tuple2<String, String>("75th Percentile of Writing SAT Scores", "SATWR75"));
		keys.add(new Tuple2<String, String>("Midpoint of Reading SAT Scores", "SATVRMID"));
		keys.add(new Tuple2<String, String>("Midpoint of Math SAT Scores", "SATMTMID"));
		keys.add(new Tuple2<String, String>("Midpoint of Writing SAT Scores", "SATWRMID"));
		keys.add(new Tuple2<String, String>("Percentage of degrees awarded in Education", "PCIP13"));
		keys.add(new Tuple2<String, String>("Percentage of degreees awarded in Engineering", "PCIP14"));
		keys.add(new Tuple2<String, String>("Percentage of degrees awarded in Biology", "PCIP26"));
		keys.add(new Tuple2<String, String>("Percentage of degrees awarded in Math", "PCIP27"));
		keys.add(new Tuple2<String, String>("Percentage of degrees awarded in Social Sciences", "PCIP45"));
		keys.add(new Tuple2<String, String>("Percentage of degrees awarded in Business", "PCIP52"));
		keys.add(new Tuple2<String, String>("Undergraduate Student Population", "UGDS"));
		keys.add(new Tuple2<String, String>("Percent of undergradute students who are white", "UGDS_WHITE"));
		keys.add(new Tuple2<String, String>("Percent of undergradute students who are black", "UGDS_BLACK"));
		keys.add(new Tuple2<String, String>("Percent of undergradute students who are Hispanic", "UGDS_HISP"));
		keys.add(new Tuple2<String, String>("Percent of undergradute students who are Asian", "UGDS_ASIAN"));
		keys.add(new Tuple2<String, String>("In-State Tuition", "TUITIONFEE_IN"));
		keys.add(new Tuple2<String, String>("Out-of-State Tuition", "TUITIONFEE_OUT"));
		keys.add(new Tuple2<String, String>("Net tuition revenue per full time student", "TUITFTE"));
		keys.add(new Tuple2<String, String>("Instructional expenditures per full time student", "INEXPFTE"));
		keys.add(new Tuple2<String, String>("Average faculty salary", "AVGFACSAL"));
		keys.add(new Tuple2<String, String>("Graduation rate for first-time students", "C150_4"));
		keys.add(new Tuple2<String, String>("Retention rate for first-time students", "RET_FT4"));
	}
	
	private boolean tryParse(String str)
	{
		try
		{
			Integer.parseInt(str);
			return true;
		}
		catch (NumberFormatException e)
		{
			return false;
		}
	}
}
