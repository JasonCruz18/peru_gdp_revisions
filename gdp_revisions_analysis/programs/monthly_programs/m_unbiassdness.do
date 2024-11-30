/********************
Summary of Statistics (Unbiassdness)
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: unbiassdness.do
		** 	First Created: 10/28/24
		** 	Last Updated:  11/##/24
			
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
	//shell mkdir "output/data" 		// Creates folder to save data.
		
	
	* Set as global vars
	
	//global graphs_folder "output/charts"	// Use to export charts.
	global tables_folder "output/tables"	// Use to export tables.
	//global data_folder "output/data"		// Use to export .dta.
	
		
		
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
		
		
	odbc load, exec("select * from sectorial_gdp_monthly_cum_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save errors_ts, replace
	
	
	odbc load, exec("select * from sectorial_gdp_monthly_int_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save revisions_ts, replace
	
	
	/*----------------------
	On-the-fly data cleaning
	(1/2)
	-----------------------*/

	
	use errors_ts, clear
	
	
		* Format the date variable
		
		** Step 1: Divide by 1000 to convert from milliseconds to seconds
		
		gen vintages_seconds = vintages_date / 1000
	
		** Step 2: Convert seconds to a Stata date using the %tc format
		
		gen vintages_stata_date = vintages_seconds / 86400 // 86400 seconds in one day

		** Step 3: Format the new variable
		
		format vintages_stata_date %td
		
		** Step 4: Convert Stata date to monthly format
		
		gen vintages_monthly = mofd(vintages_stata_date)
		//gen vintages_quarterly = qofd(vintages_stata_date) // quarterly
		//gen vintages_annual = yofd(vintages_stata_date) // annual
		
		** Step 5: Monthly formatting
		
		format vintages_monthly %tm
		//format vintages_quarterly %tq // quarterly
		//format vintages_annual %ty // annual

	
	save errors_ts_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(2/2)
	-----------------------*/

	
	use revisions_ts, clear
	
	
		* Format the date variable
		
		** Step 1: Divide by 1000 to convert from milliseconds to seconds
		
		gen vintages_seconds = vintages_date / 1000
	
		** Step 2: Convert seconds to a Stata date using the %tc format
		
		gen vintages_stata_date = vintages_seconds / 86400 // 86400 seconds in one day

		** Step 3: Format the new variable
		
		format vintages_stata_date %td
		
		** Step 4: Convert Stata date to monthly format
		
		gen vintages_monthly = mofd(vintages_stata_date)
		//gen vintages_quarterly = qofd(vintages_stata_date) // quarterly
		//gen vintages_annual = yofd(vintages_stata_date) // annual
		
		** Step 5: Monthly formatting
		
		format vintages_monthly %tm
		//format vintages_quarterly %tq // quarterly
		//format vintages_annual %ty // annual

	
	save revisions_ts_cleaned, replace
	
	
	
	/*----------------------
	e: summary of stats
	(mean with significance)
	-----------------------*/

	
	use errors_ts_cleaned, clear
	
		
		* Define in the macro `$sectors`

	global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
	
	
		* Create a new frame named `stats_sum_e` to store regression results and summary statistics

		frame create stats_sum_e str32 variable int n str32 coef str8 sd str8 p1 str8 p99
		

		* Iterate over all sectors defined in the macro `$sectors`

		foreach sector in $sectors {
			
			** Loop through variables e_#_<sector> where # ranges from 1 to 19
			
			forval i = 1/12 {
				
				capture confirm variable e_`i'_`sector' // Check if the variable e_<i>_<sector> exists
				
				if !_rc { // If the variable exists (_rc == 0)
					
					capture {
						
						tsset vintages_monthly
					
						newey e_`i'_`sector', lag(1) force // Regression
						
						if _rc == 2001 { // If the regression fails due to insufficient observations
						
							di in red "Insufficient observations for e_`i'_`sector'"
							continue
						}
						
						*** Store regression result matrix
						
						matrix M = r(table)
						
						*** Calculate detailed summary statistics for the variable
						
						summarize e_`i'_`sector', detail
						local n = r(N)                   	// Number of observations
						local sd = string(r(sd), "%9.3f")  	// Standard deviation
						local p1 = string(r(p1), "%9.3f")  	// 1st percentile
						local p99 = string(r(p99), "%9.3f") // 99th percentile
						
						*** Extract the constant term coefficient and its p-value
						
						local coef = M["b", "_cons"]
						local pvalue = M["pvalue", "_cons"]
						
						*** Format the coefficient string with significance stars based on the p-value
						
						if `pvalue' < 0.01 {
							local coef = string(`coef', "%9.3f") + "***"
						}
						else if `pvalue' >= 0.01 & `pvalue' < 0.05 {
							local coef = string(`coef', "%9.3f") + "**"
						}
						else if `pvalue' >= 0.05 & `pvalue' < 0.10 {
							local coef = string(`coef', "%9.3f") + "*"
						}
						else {
							local coef = string(`coef', "%9.3f")
						}
						
						*** Post the variable name, summary statistics, and formatted coefficient to the results frame
						
						frame post stats_sum_e ("e_`i'_`sector'") (`n') ("`coef'") ("`sd'") ("`p1'") ("`p99'")
					}
				}
				
				else {
					di in yellow "Variable e_`i'_`sector' does not exist" // If the variable does not exist, display a warning message
				}
			}
		}


		* Switch to the `stats_sum_e` frame to view the stored results

		frame change stats_sum_e

		
		* List the results without observation numbers and in a clean format

		list variable n coef p1 p99 sd, noobs clean
	
	
	
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
	
	