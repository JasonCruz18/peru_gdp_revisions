/********************
Summary of Statistics (Unbiassdness)
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: jefas_unbiassdness.do
		** 	First Created: 03/20/25
		** 	Last Updated:  03/21/25
			
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
			
	//log using jefas_unbiassdness.txt, text replace // Opens a log file and replaces it if it exists.

	

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
		
		
	odbc load, exec("select * from e_gdp_monthly_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change the dataset loaded from SQL as preferred. 
		
	
	save gdp_releases, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(GDP releases)
	-----------------------*/

		
	use gdp_releases, clear
	
	
		* Remove the current definitive value (most recent release) for each target period and sector
		
		drop *_most_recent
		
		
		* Remove columns for h>12
		
		ds *_release_*
		
		foreach var in `r(varlist)' {
			if regexm("`var'", "([0-9]+)$") { // Extract the number at the end of the variable name
				local num = regexs(1)  // We use regexs(1) to capture the number

				if real("`num'") > 12 { // Check if "num" is greater than 12
					drop `var'
				}
			}
		}
				
				
		* Redefine the 12th release as the definitive value of GDP growth
		
		rename gdp_release_12 gdp_most_recent
	
	
		* Format the date variable
		
		replace vintages_date = mofd(dofc(vintages_date))
		format vintages_date %tm
		
		
		* Set vintages_date as first column
		
		order vintages_date
		
		
		* Sort by vintages_date
		
		sort vintages_date
		
		
		* Keep obs in specific date range
		
		keep if vintages_date > tm(1992m12) & vintages_date < tm(2023m11)
		
	
	save gdp_releases_cleaned, replace
	
	
	
	/*----------------------
	Compute prediction
	errors (e)
	-----------------------*/

	
	use gdp_releases_cleaned, clear
	
	
		* Generate forecast error for each horizon and sector
		
		forval i = 1/11 {
			gen e_`i'_gdp = gdp_most_recent - gdp_release_`i'
		}
		
	
	save e_gdp_releases, replace
	
	
	
	/*----------------------
	e: summary of stats
	(mean with significance)
	________________________
	Paper and presentation
	version
	-----------------------*/

	
	use e_gdp_releases, clear
		
		
		* Create a new frame named `stats_sum_e` to store regression results and summary statistics

		frame create stats_sum_e str32 variable int n str32 coef str8 sd str8 p1 str8 p99

			
		* Loop through variables e_`i'_gdp where `i' ranges from 1 to 11
		
		forval i = 1/11 {
			
			capture confirm variable e_`i'_gdp // Check if the variable e_`i'_gdp exists
			
			if !_rc { // If the variable exists (_rc == 0)
				
				capture {
					
					tsset vintages_date
				
					newey e_`i'_gdp, lag(1) force // Regression
					
					if _rc == 2001 { // If the regression fails due to insufficient observations
					
						di in red "Insufficient observations for e_`i'_gdp"
						continue
					}
					
					*** Store regression result matrix
					
					matrix M = r(table)
					
					*** Calculate detailed summary statistics for the variable
					
					summarize e_`i'_gdp, detail
					local n = r(N)                   	// Number of observations
					local sd = string(r(sd), "%9.2f")  	// Standard deviation
					local p1 = string(r(p1), "%9.2f")  	// 1st percentile
					local p99 = string(r(p99), "%9.2f") // 99th percentile
					
					*** Extract the constant term coefficient and its p-value
					
					local coef = M["b", "_cons"]
					local se = M["se", "_cons"]
					local pvalue = M["pvalue", "_cons"]
					
					*** Format the coefficient string with significance stars based on the p-value
					
					if `pvalue' < 0.01 {
						local coef = string(`coef', "%9.2f") + "***"
					}
					else if `pvalue' >= 0.01 & `pvalue' < 0.05 {
						local coef = string(`coef', "%9.2f") + "**"
					}
					else if `pvalue' >= 0.05 & `pvalue' < 0.10 {
						local coef = string(`coef', "%9.2f") + "*"
					}
					else {
						local coef = string(`coef', "%9.2f")
					}
					
					*** Append standard error in parentheses to coef
					local coef = "`coef' (" + string(`se', "%9.2f") + ")"
					
					*** Post the variable name, summary statistics, and formatted coefficient to the results frame
					
					frame post stats_sum_e ("e_`i'_gdp") (`n') ("`coef'") ("`sd'") ("`p1'") ("`p99'")
				}
			}
			
			else {
				di in yellow "Variable e_`i'_gdp does not exist" // If the variable does not exist, display a warning message
			}
		}


		* Switch to the `stats_sum_e` frame to view the stored results

		frame change stats_sum_e

		
		* List the results without observation numbers and in a clean format

		list variable n coef p1 p99 sd, noobs clean
		
		
		* Rename vars
		
		rename variable h
		rename coef Insesgadez
		rename p1 P1
		rename p99 P99
		rename sd SD
		
		
		* Order vars
		
		order h n Insesgadez P1 P99 SD
	
		
		* Export to excel file
		
		export excel using "$tables_folder/gdp_e_unbiassdness.xlsx", ///
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
	
	