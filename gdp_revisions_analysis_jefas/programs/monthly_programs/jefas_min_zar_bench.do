/********************
Mincer–Zarnowitz Regressions
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: jefas_min_zar_bench.do
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
			
	//log using jefas_min_zar_bench.txt, text replace // Opens a log file and replaces it if it exists.

	

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
		
		
	odbc load, exec("select * from r_gdp_monthly_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change the dataset loaded from SQL as preferred. 
		
	
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
	Compute ongoing
	revisions (r)
	-----------------------*/
	
	
	use gdp_releases_cleaned, clear
	
	
		* Generate ongoing revisions for each horizon and sector
		
		forval i = 2/11 {
			gen r_`i'_gdp = gdp_release_`i' - gdp_release_`=`i'-1'
		}
		
		
		* Compute final revision (12th horizon)
		
		gen r_12_gdp = gdp_most_recent - gdp_release_11				

	
	save r_gdp_releases, replace
	
	
	
	/*----------------------
	Compute prediction
	errors (e)
	-----------------------*/

	
	use r_gdp_releases, clear
	
	
		* Generate forecast error for each horizon and sector
		
		forval i = 1/11 {
			gen e_`i'_gdp = gdp_most_recent - gdp_release_`i'
		}
		
	
	save r_e_gdp_releases, replace
	
	

	/*----------------------
	Compute prediction
	errors (z)
	-----------------------*/

	
	use r_e_gdp_releases, clear
	
	
		* Generate forecast error for each horizon and sector	
		
		forval i = 2/11 {
			gen z_`i'_gdp = gdp_release_`i' - gdp_release_1
		}
	
	
		* Compute final revision (12th horizon)
		
		gen z_12_gdp = gdp_most_recent - gdp_release_1
		
	
	save r_e_z_gdp_releases, replace
	export delimited using "jefas_gdp_revisions.csv", replace
	
	
	
	/*----------------------
	y: Mincer–Zarnowitz
	________________________
	Paper and presentation
	version
	-----------------------*/

	
	use r_e_z_gdp_releases, clear
		
	
		* Create a new frame named `y_min_zar` to store regression results
		frame create y_min_zar str32 variable int n double constant double coef str32 test_result double pvalue

		* Loop through variables gdp_release_`i' where `i` ranges from 1 to 11
		forval i = 1/11 {

			capture confirm variable gdp_release_`i' // Check if the variable exists

			if !_rc { // If the variable exists

				capture {
					tsset vintages_date

					* Run regression with Newey-West standard errors
					newey gdp_most_recent gdp_release_`i', lag(1) force

					if _rc == 2001 { // If regression fails due to insufficient observations
						di in red "Insufficient observations for gdp_release_`i'"
						continue
					}

					* Extract regression results
					matrix M = r(table)
					local constant = M["b", "_cons"]
					local coef = M["b", "gdp_release_`i'"]

					* Perform the hypothesis test: H0: constant = 0 & coef = 1
					test (_cons = 0) (gdp_release_`i' = 1)

					if _rc == 0 { // If the test command succeeds
						local pvalue = r(p)

						* Extract number of observations
						summarize gdp_most_recent, detail
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
						frame post y_min_zar ("gdp_release_`i'") (`n') (`constant') (`coef') ("`test_result'") (`pvalue')
					}
					else {
						di in red "Test failed for gdp_release_`i'"
					}
				}
			}
			else {
				di in yellow "Variable gdp_release_`i' does not exist"
			}
		}

		* Switch to the `y_min_zar` frame to view the stored results
		frame change y_min_zar

		* List the results without observation numbers and in a clean format
		list variable n constant coef test_result pvalue, noobs clean
				
				
		* Rename vars
		
		rename variable h
		rename constant Intercepto
		rename coef Beta
		
		
		* Order vars
		
		order h n Intercepto Beta
	
		
		* Export to excel file
		
		export excel using "$tables_folder/gdp_releases_min_zar.xlsx", ///
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
	
	