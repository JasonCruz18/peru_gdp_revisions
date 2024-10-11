	/********************
	Efficiency cross sectors
	***

			Author
			---------------------
			Jason Cruz
			*********************/

			*** Program: efficiency_cross_sectors.do
			** 	First Created: 10/11/24
			** 	Last Updated:  10/11/24
				
		***
		** Just click on the "Run (do)" button, the code will do the rest for you.
		***

		
		
		/*----------------------
		Initial script configuration
		-----------------------*/

		cls 					// Clears the screen.
		clear all 				// Frees all memory.
		version 				// Displays the software version.
			
		set more off 			// Turns off pagination for output.
		cap set maxvar 12000	// Sets the maximum number of variables to 12000.
		program drop _all 		// Deletes all user-defined programs.
				
		capture log close 		// Closes the log file if open.
					
		pause on 				// Enables pauses in programs.
		set varabbrev off 		// Turns off variable abbreviation.
				
		//log using efficiency_tests.txt, text replace // Opens a log file and replaces it if it exists.

		

		/*----------------------
		Defining workspace path
		------------------------*/
		
		di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER."'  _request(path)
		
		cd "$path"
			
			
			
		/*----------------------
		Setting folders to save outputs
		------------------------*/
			
		shell mkdir "output" 			// Creates folder to save outputs.
		//shell mkdir "output/graphs" 	// Creates folder to save graphs.
		shell mkdir "output/tables" 	// Creates folder to save tables.
		//shell mkdir "output/data" 		// Creates folder to save data.
			
		
		* Set as global vars
		
		//global graphs_folder "output/graphs"	// Use to export graphs.
		global tables_folder "output/tables"	// Use to export tables.
		//global data_folder "output/data"		// Use to export .dta.
		
			
			
		/*----------------------
		Import ODBC dataset and
		save temp
		-----------------------*/
			
			
		odbc load, exec("select * from sectorial_gdp_monthly_int_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
			
		
		save temp_panel_data, replace


		
		/*----------------------
		On-the-fly data cleaning
		-----------------------*/

		
		use temp_panel_data, clear

		
			* Order and sort
			
			sort vintages_date horizon // Key step to set both the ID and time vars for panel data.

			
			* At a glance (inspect data)

			d // Check entire dataset vars to understand its structure.
			sum // Summarize stats for all vars in the dataset (mean, standard deviation, etc.).
			count // Count the total number of observations, expected to be 6,696.

			
			* Fixing date format
			
			gen numeric_date = dofc(vintages_date) // To a Stata date in days.
			format numeric_date %td // To standard Stata date (e.g., day-month-year).

			gen target_date = mofd(numeric_date) // To a monthly date format.
			format target_date %tm // To standard Stata month (e.g., Jan 2023).

			drop vintages_date numeric_date // Drop the original vars since they are no longer needed.

			order target_date horizon // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.

			
			
		/*----------------------
		Regression (nowcast error)
		-----------------------*/