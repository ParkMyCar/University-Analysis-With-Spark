import java.util.*;

public class UserInterface 
{
	Analyzer analyzer = new Analyzer();
	
	//Public Methods
	public UserInterface()
	{
		/*<TODO> At a later date. Add a settings file that will contain settings or preferences
		 * ie. Location, Max Tuition, School Size, Major, could help in a search
		 */
		DataManager.getDataManager();
		String title = " __   __ __    _ ___ __   __ _______ ______   _______ ___ _______ __   __   _______ __    _ _______ ___    __   __ _______ ___ _______\n"
				     + "|  | |  |   |_| |   |  |_|  |    ___|   | || |  _____|   |_     _|  |_|  | |  |_|  |   |_| |  |_|  |   |  |  |_|  |  _____|   |  _____|\n"
				     + "|  |_|  |       |   |       |   |___|   |_||_| |_____|   | |   | |       | |       |       |       |   |  |       | |_____|   | |_____ \n"
				     + "|       |  _    |   |       |    ___|    __  |_____  |   | |   | |_     _| |       |  _    |       |   |__|_     _|_____  |   |_____  |\n"
				     + "|       | | |   |   ||     ||   |___|   |  | |_____| |   | |   |   |   |   |   _   | | |   |   _   |       ||   |  _____| |   |_____| |\n"
				     + "|_______|_|  |__|___| |___| |_______|___|  |_|_______|___| |___|   |___|   |__| |__|_|  |__|__| |__|_______||___| |_______|___|_______|";
		System.out.println(title + "\n");
		System.out.print("Welcome! Please pick one of the options below: \n"
				+ "\t1: Seach for a school\n"
				+ "\t2: Top - 50 search based on a statistic\n"
				+ "\t3: View trend over time\n"
				+ "\t4: Correlation between two statistics\n"
				+ "\t5: Exit\n\n");
	}
	
	public void getInput()
	{
		Scanner kbd = new Scanner(System.in);
		int function = 0;
		String[] args = new String[4];
		while(function != 5) 
		{
			System.out.print("Selection (number): ");
			if (kbd.hasNextInt()) //Verifies the input is an integer
			{ 
				function = kbd.nextInt();
			}
			kbd.nextLine();
			
			switch (function) {
			case 1: //Search for school
				System.out.print("School to search for: ");
				args[1] = kbd.nextLine();
				break;
			case 2: //Top - 10 Search
				//showStatistics();
				System.out.print("Top 50 schools in: ");
				args[1] = kbd.nextLine();
				break;
			case 3: //Trend over time
				System.out.print("School to analyze : ");
				args[1] = kbd.nextLine(); //School
				System.out.print("Statistic to analyze: ");
				args[2] = kbd.nextLine(); //Statistic
				break;
			case 4: //Correlation between two statistics
				System.out.print("School to analyze: ");
				args[1] = kbd.nextLine(); //School
				System.out.print("First statistic: ");
				args[2] = kbd.nextLine(); //First statistic for correlation
				System.out.print("Second statistic: ");
				args[3] = kbd.nextLine(); //Second statistic for correlation
				break;
			case 5: //Exit
				function = 5;
				break;
			default:
				System.out.println("Command not recognized! Please enter the number of the function you would like to perform.\n");
				break;
			}
			args[0] = String.valueOf(function);
			
			handleOutput(analyzer.analyze(args), function);
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
			default:
				break;
		}
	}
}
