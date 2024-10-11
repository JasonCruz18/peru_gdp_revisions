/********************
Variance Bounds Tests
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: variance_bounds_tests.do
		** 	First Created: 09/15/24
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
		
		
	odbc load, exec("select * from sectorial_gdp_monthly_cum_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Loads data from PostgresSQL using ODBC.
		
	
	save temp_cum_panel_data, replace


	
	/*----------------------
	On-the-fly data cleaning
	-----------------------*/

	
	use temp_cum_panel_data.dta, clear

	
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
		
		
		* Generate time-trend var
		
		** Get max value from horizon
		egen max_horizon = max(horizon)
		
		** Gen new var as the difference between max_horizon and horizon
		gen time_trend = max_horizon - horizon // This is a kind of trend var (H-j)
		

		
	/*----------------------
	Regression (nowcast error
	abs value) [1/2]
	-----------------------*/
	
	
		* Setting up the panel data structure
		
		xtset target_date horizon // Using 'target_date' as the time var and 'horizon' as the panel id.
		
		
		* Extract all variables that start with 'e_'
		
		ds e_*
	
	
		* Standard errors corrected for Newey West
		** Run regression (abs value)
		*** Loop through each sector and run regressions
		foreach var of varlist e_* {
    
			**** Create a new variable with the absolute value
			gen abs_`var' = abs(`var')
			
			**** Run the Newey-West regression
			newey abs_`var' time_trend, lag(2) force
		}
		
		
		* Standard errors corrected for Newey West
		** Run regression (square)
		*** Loop through each sector and run regressions
		foreach var of varlist e_* {

			**** Create a new variable with the squared value
			gen sq_`var' = `var'^2

			**** Run the Newey-West regression with the squared variable
			newey sq_`var' time_trend, lag(2) force
		}
				
		
		
	/*----------------------
	Regression (nowcast error
	abs value) [2/2]
	-----------------------*/
	
	
		* Setting up the panel data structure
		
		xtset target_date horizon // Using 'target_date' as the time var and 'horizon' as the panel id.
		
		
		* Extract all variables that start with 'e_'
		
		ds e_*
		
		
		* Gen square of time trend varabbrev
		
		gen sq_time_trend = time_trend^2
	
	
		* Standard errors corrected for Newey West
		** Run regression (abs value)
		*** Loop through each sector and run regressions
		foreach var of varlist e_* {
    
			**** Create a new variable with the absolute value
			gen abs_`var' = abs(`var')
			
			**** Run the Newey-West regression
			newey abs_`var' sq_time_trend, lag(2) force
		}
		
		
		* Standard errors corrected for Newey West
		** Run regression (square)
		*** Loop through each sector and run regressions
		foreach var of varlist e_* {

			**** Create a new variable with the squared value
			gen sq_`var' = `var'^2

			**** Run the Newey-West regression with the squared variable
			newey sq_`var' sq_time_trend, lag(2) force
		}
	
		
		
	/*----------------------
	Drop aux data (.dta)
	-----------------------*/	

	// List all .dta files in the current directory and store in a local macro
	local dta_files : dir . files "*.dta"

	// Iterate over each .dta file and delete it
	foreach file of local dta_files {
		erase "`file'"
	}	

	
	
	
	
	
	
	
	
	
	
	
	
	
	