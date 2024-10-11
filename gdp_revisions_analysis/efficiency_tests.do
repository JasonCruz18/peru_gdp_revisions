	/********************
	Efficiency Tests
	***

			Author
			---------------------
			Jason Cruz
			*********************/

			*** Program: efficiency_tests.do
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
			
			
		odbc load, exec("select * from sectorial_gdp_annual_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
			
		
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
		
		
			* Setting up the panel data structure
			
			xtset target_date horizon // Using 'target_date' as the time var and 'horizon' as the panel id.
			
			
			* Extract the suffixes after "e_" to build the list of sectors
			ds e_*

			* Create a list of suffixes
			local sectors ""

			* Loop through the matched variables and extract the suffix
			foreach var of varlist e_* {
				local suffix = substr("`var'", 3, .)  // Remove the first 2 characters "e_"
				local sectors "`sectors' `suffix'"    // Append each suffix to the sectors list
			}

			* Display the sectors for debugging purposes
			display "Sectors: `sectors'"

			* Standard errors NOT corrected
			** Run fixed effects regression
			*** Loop through each sector and run regressions
			foreach var in `sectors' {
				**** Display current sector
				display "Running fixed effects regression for sector `var'"
				
				**** Run regression for each sector
				xtreg e_`var' L1.r_`var' L2.r_`var', fe vce(cluster target_date) // Run fixed effects regression for the current sector
			} 

			* Standard errors corrected for Newey-West
			** Run regression
			*** [PENDING] Loop through each sector and run regressions
			
			**** Aux
				
				newey e_commerce L1.r_commerce, lag(0) force
				newey e_services L1.r_services, lag(1) force
				newey e_mining L1.r_mining, lag(1) force
				newey e_electricity L1.r_electricity, lag(1) force
				newey e_gdp L1.r_gdp, lag(1) force
				newey e_agriculture L1.r_agriculture, lag(1) force
				newey e_construction L1.r_construction, lag(1) force
				newey e_fishing L1.r_fishing, lag(1) force
				newey e_manufacturing L1.r_manufacturing L2.r_manufacturing, lag(3) force	
				
		
		
			/*----------------------
			Post-estimation inspection
			-----------------------*/
		
			// 1. Specify and fit a fixed effects model with robust errors.
			xtreg e_manufacturing L1.r_manufacturing L2.r_manufacturing, fe  vce(cluster target_date)
			
			//xtivreg2 e_manufacturing L1.r_manufacturing L2.r_manufacturing, fe bw(1) robust // ssc install xtivreg2


			// 2. Running a Hausman test
			xtreg e_manufacturing L1.r_manufacturing L2.r_manufacturing, fe
			estimates store fe_model
			xtreg e_manufacturing L1.r_manufacturing L2.r_manufacturing, re
			estimates store re_model
			hausman fe_model re_model
			
			suest fe_model re_model


			// 3. Fitting the Newey-West model
			// newey e_manufacturing L1.r_manufacturing L2.r_manufacturing, lag(1) force

			
		
		/*----------------------
		Import ODBC dataset and
		save temp
		-----------------------*/
			
			
		odbc load, exec("select * from sectorial_gdp_annual_int_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
			
		
		save temp_inter_panel_data, replace
			
			
			
		/*----------------------
		On-the-fly data cleaning
		-----------------------*/

		
		use temp_inter_panel_data, clear
		
		
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
		Regression (intermediate
		revisions)
		-----------------------*/
			
			
			* Setting up the panel data structure
			
			xtset target_date horizon // Using 'target_date' as the time var and 'horizon' as the panel id.
			
			* Extract all variables that start with 'r_'
				
			ds r_*
				
			
			* Standard errors NOT corrected
			** Run fixed effects regression
			*** Loop through each sector and run regressions
			foreach var of varlist r_* {
				
				**** Run regression for each sector
				xtreg `var' L1.`var' L2.`var', fe // Run fixed effects regression for the current sector
			} 
			
			
			* Standard errors corrected for Newey West
			** Run regression
			*** Loop through each sector and run regressions
			foreach var of varlist r_* {
				
				**** Run regression for each sector
				newey `var' L1.`var' L2.`var', lag(2) force // Using Newey-West
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
		
		