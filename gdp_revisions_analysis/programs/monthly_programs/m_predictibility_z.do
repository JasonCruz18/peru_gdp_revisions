/********************
Predictibility (e)
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: m_predictibility_e.do
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
		
		
		* Create a new frame named `z_predictibility` to store regression results
		
		frame create z_predictibility str32 variable int n str32 constant str32 coef
		

		* Iterate over all sectors defined in the macro `$sectors`

		foreach sector in $sectors {
			
			** Loop through variables r_#_<sector> where # ranges from 2 to 10
			
			forval i = 2/12 {
				
				capture confirm variable z_`i'_`sector' // Check if the variable z_<i>_<sector> exists
				
				if !_rc { // If the variable exists (_rc == 0)
					
					capture {
						
						tsset target_date
					
						newey z_`i'_`sector' r_2_`sector', lag(1) force // Regression
						
						if _rc == 2001 { // If the regression fails due to insufficient observations
						
							di in red "Insufficient observations for r_`i'_`sector'"
							continue
						}
						
						*** Store regression result matrix
						
						matrix M = r(table)
						
						*** Calculate detailed summary statistics for the variable
						
						summarize z_`i'_`sector', detail
						local n = r(N)                   	// Number of observations
						
						*** Extract the constant term coefficient and its p-value
						
						local constant = M["b", "_cons"]
						local constant_pvalue = M["pvalue", "_cons"]
						
						local coef = M["b", "r_2_`sector'"]
						local coef_pvalue = M["pvalue", "r_2_`sector'"]
						
						*** Format the coefficient string with significance stars based on the p-value
						
						if `constant_pvalue' < 0.01 {
							local constant = string(`constant', "%9.3f") + "***"
						}
						else if `constant_pvalue' >= 0.01 & `constant_pvalue' < 0.05 {
							local constant = string(`constant', "%9.3f") + "**"
						}
						else if `constant_pvalue' >= 0.05 & `constant_pvalue' < 0.10 {
							local constant = string(`constant', "%9.3f") + "*"
						}
						else {
							local constant = string(`constant', "%9.3f")
						}
						
						
						if `coef_pvalue' < 0.01 {
							local coef = string(`coef', "%9.3f") + "***"
						}
						else if `coef_pvalue' >= 0.01 & `coef_pvalue' < 0.05 {
							local coef = string(`coef', "%9.3f") + "**"
						}
						else if `coef_pvalue' >= 0.05 & `coef_pvalue' < 0.10 {
							local coef = string(`coef', "%9.3f") + "*"
						}
						else {
							local coef = string(`coef', "%9.3f")
						}
						
						*** Post the variable name, summary statistics, and formatted coefficient to the results frame
						
						frame post z_predictibility ("z_`i'_`sector'") (`n') ("`constant'") ("`coef'")
					}
				}
				
				else {
					di in yellow "Variable z_`i'_`sector' does not exist" // If the variable does not exist, display a warning message
				}
			}
		}


		* Switch to the `z_predictibility` frame to view the stored results

		frame change z_predictibility

		
		* List the results without observation numbers and in a clean format

		list variable n constant coef, noobs clean
	
		
		* Export to excel file
		
		export excel using "z_predictibility_m.xlsx", ///
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
	
	