/********************
Predictibility (y)
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: m_predictibility_y.do
		** 	First Created: 12/03/24
		** 	Last Updated:  12/--/24
			
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
	//shell mkdir "output/charts" 	// Creates folder to save charts.
	shell mkdir "output/tables" 	// Creates folder to save tables.
	//shell mkdir "output/data" 	// Creates folder to save data.
		
	
	* Set as global vars
	
	//global graphs_folder "output/charts"	// Use to export charts.
	global tables_folder "output/tables"	// Use to export tables.
	//global data_folder "output/data"		// Use to export .dta.
	
		
		
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
	
	
	odbc load, exec("select * from r_sectorial_gdp_monthly") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save r_ts, replace
	
	
	odbc load, exec("select * from e_sectorial_gdp_monthly") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save e_ts, replace
	
	
	odbc load, exec("select * from z_sectorial_gdp_monthly") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save z_ts, replace
	
	

	/*----------------------
	On-the-fly data cleaning
	(r)
	-----------------------*/

	
	use r_ts, clear

	
		* Sort by vintages_date
		
		sort vintages_date // Key step to set both the ID and time vars for panel data.

		
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

		
		order target_date // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.


		* Sort by target_date and horizon
		
		sort target_date
	
	
	save r_ts_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(e)
	-----------------------*/

	
		use e_ts, clear

	
		* Sort by vintages_date
		
		sort vintages_date // Key step to set both the ID and time vars for panel data.

		
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

		
		order target_date // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.


		* Sort by target_date and horizon
		
		sort target_date
	
	
	save e_ts_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(z)
	-----------------------*/

	
		use z_ts, clear

	
		* Sort by vintages_date
		
		sort vintages_date // Key step to set both the ID and time vars for panel data.

		
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

		
		order target_date // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.


		* Sort by target_date and horizon
		
		sort target_date
	
	
	save z_ts_cleaned, replace
	
	
	
	/*----------------------
	Merge r, e, z
	-----------------------*/
	
	
	use r_ts_cleaned, clear
	

		* Merge with the second dataset (e_panel_cleaned)
		
		merge 1:1 target_date using e_ts_cleaned

		
		* Check the merge result
		
		tab _merge // _merge values: 1 = only in master, 2 = only in using, 3 = matched

		
		* If you want to keep only the matches:
		
		keep if _merge == 3
		drop _merge
		
		
		* Merge with the third dataset (z_panel_cleaned)
		
		merge 1:1 target_date using z_ts_cleaned
		
		
		* Check the merge result
		
		tab _merge // _merge values: 1 = only in master, 2 = only in using, 3 = matched

		
		* If you want to keep only the matches:
		
		keep if _merge == 3
		drop _merge
		

	save r_e_z_ts, replace

		
		
	/*----------------------
	Predictibility (z)
	________________________
	Paper and presentation
	version
	-----------------------*/

	* z_t(h) = c + \beta_1 r_t(h=2)
	*.........................................................................

	
	use r_e_z_ts, clear
	
	
		* Define in the macro `$sectors`

		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
		
		
		* Create a new frame named `y_predictibility` to store regression results
		frame create y_predictibility str32 variable int n double constant double coef str32 test_result double pvalue

		* Iterate over all sectors defined in the macro `$sectors`
		foreach sector in $sectors {

			* Loop through variables `sector'_release_`i' where `i` ranges from 2 to 12
			forval i = 1/19 {

				capture confirm variable `sector'_release_`i' // Check if the variable exists

				if !_rc { // If the variable exists

					capture {
						tsset target_date

						* Run regression with Newey-West standard errors
						newey `sector'_most_recent `sector'_release_`i', lag(1) force

						if _rc == 2001 { // If regression fails due to insufficient observations
							di in red "Insufficient observations for `sector'_release_`i'"
							continue
						}

						* Extract regression results
						matrix M = r(table)
						local constant = M["b", "_cons"]
						local coef = M["b", "`sector'_release_`i'"]

						* Perform the hypothesis test: H0: constant = 0 & coef = 1
						test (_cons = 0) (`sector'_release_`i' = 1)

						if _rc == 0 { // If the test command succeeds
							local pvalue = r(p)

							* Extract number of observations
							summarize `sector'_most_recent, detail
							local n = r(N)

							* Determine significance based on the p-value of the joint test
							local test_result
							if `pvalue' < 0.01 {
								local test_result = "***"
							}
							else if `pvalue' >= 0.01 & `pvalue' < 0.05 {
								local test_result = "**"
							}
							else if `pvalue' >= 0.05 & `pvalue' < 0.10 {
								local test_result = "*"
							}
							else {
								local test_result = "Fail to Reject H0"
							}

							* Post results to the results frame
							frame post y_predictibility ("`sector'_release_`i'") (`n') (`constant') (`coef') ("`test_result'") (`pvalue')
						}
						else {
							di in red "Test failed for `sector'_release_`i'"
						}
					}
				}
				else {
					di in yellow "Variable `sector'_release_`i' does not exist"
				}
			}
		}

		* Switch to the `y_predictibility` frame to view the stored results
		frame change y_predictibility

		* List the results without observation numbers and in a clean format
		list variable n constant coef test_result pvalue, noobs clean
				
				
		* Rename vars
		
		rename variable h
		rename constant Intercepto
		rename coef Beta
		
		
		* Order vars
		
		order h n Intercepto Beta
	
		
		* Export to excel file
		
		export excel using "$tables_folder/y_predictibility_m.xlsx", ///
    firstrow(variable) replace
					
						
	
	/*----------------------
	Drop aux data and tables
	-----------------------*/	

	
	* List all .dta, .txt and .tex files in the current directory and store in a local macro
	
	local dta_files : dir . files "*.dta"
	//local txt_files : dir . files "*.txt"
	//local tex_files : dir . files "*.tex"

	
	* Iterate over each .dta file and delete it
	
	foreach file of local dta_files {
		erase "`file'"
	}	
	
	
	* Iterate over each .txt file and delete it
	
	//foreach file of local txt_files {
	//	erase "`file'"
	//}	
	
	
	* Iterate over each .tex file and delete it
	
	//foreach file of local tex_files {
	//	erase "`file'"
	//}	
	
	