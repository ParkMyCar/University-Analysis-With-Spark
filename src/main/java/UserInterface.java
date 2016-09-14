import java.util.*;

public class UserInterface 
{
	Analyzer analyzer = new Analyzer();
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
				+ "\t2: Top - 10 search based on a statistic\n"
				+ "\t3: Exit\n\n");
	}
	
	public void getInput()
	{
		Scanner kbd = new Scanner(System.in);
		int function = 0;
		String input = "";
		String[] args;
		while(function != 3)
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
				input = kbd.nextLine();
				break;
			case 2: //Top - 10 Search
				//showStatistics();
				System.out.print("Top 50 schools in: ");
				input = kbd.nextLine();
				break;
			case 3: //Exit
				function = 3;
				break;
			default:
				System.out.println("Command not recognized! Please enter the number of the function you would like to perform.\n");
				break;
			}
			args = new String[2];
			args[0] = String.valueOf(function);
			args[1] = input;
			
			handleOutput(analyzer.analyze(args), function);
		}
		kbd.close();
		DataManager.getDataManager().cleanUp();
	}
	
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
			default:
				break;
		}
	}
	
	private void showStatistics()
	{
		System.out.println("Find the top - 10 schools for the following statistics: ");
		Set<String> keys = analyzer.headerMap.keySet();
		int index = 1;
		for (String str : keys)
		{
			if (index++ % 15 == 0)
			{ System.out.print("\n"); }
			System.out.print(str + " ");
			
		}
	}
	
}
