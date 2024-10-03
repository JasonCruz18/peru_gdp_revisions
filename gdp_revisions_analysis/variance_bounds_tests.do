/********************
Variance Bounds Tests
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: variance_bounds_tests.do
		** 	First Created: 09/15/24
		** 	Last Updated:  10/03/24
			
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
			
	//log using twin_deficits_weo_wdi.txt, text replace // Opens a log file and replaces it if it exists.

	

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
	Import ODBC merged dataset and
	save temp
	-----------------------*/
											
	odbc load, exec("select * from cum_monthly_fixed_event_part_0") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Loads WEO, WDI and PF merged from PostgresSQL using ODBC.
		
	
	save fixed_event_temp, replace


	
	/*----------------------
	calculate absolute value
	and square of regressor
	-----------------------*/
	
	* Abs value
	gen abs_electricity_2020_01 = abs(electricity_2020_01)

	* Square
	gen square_electricity_2020_01 = electricity_2020_01^2

	
	/*----------------------
	OLS Regression
	-----------------------*/
	
	// Please, install Newey if is needed
	
	//ssc install ivreg2
	
	* Run the regression
	
	ivreg2 abs_electricity_2020_01 time_trend, robust

	
	
	
	
	
	
	
	
	
	
	
	
	
	