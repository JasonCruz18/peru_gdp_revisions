/********************
Data Clean-up
***

		Author(s)
		---------------------
		D & J
		*********************/

		*** Program: data_cleansing.do
		** 	First Created: 07/11/25
		** 	Last Updated:  07/12/25
			
	***
	** Just click on the "Run (do)" button, the code will do the rest for you.
	***
	
	

	/*----------------------
	Initial do-file setting
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
			
*	log using jefas_encompassing.txt, text replace // Opens a log file and replaces it if it exists.



	/*----------------------
	Defining workspace path
	------------------------*/
		
		di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER."'  _request(path)
		
		cd "$path"
			
			
			
	/*----------------------
	Setting folders to save outputs
	------------------------*/
	
	shell mkdir "input"			// Creating input folder.
	shell mkdir "input/data"	// Creating input data folder.
	shell mkdir "output" 		// Creating output folder.
*	shell mkdir "output/graphs" // Creating output charts folder.
	shell mkdir "output/tables" // Creating output tables folder.
*	shell mkdir "output/data" 	// Creating output data folder.
			
		
	* Set as global vars
		
	global input_data "input/data"			// Use to import data (gdp_releases.dta).
*	global output_graphs "output/graphs"	// Use to export charts.
	global output_tables "output/tables"	// Use to export tables.
	
	
	cd "$input_data"
			
			
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
		
		
	odbc load, exec("select * from e_gdp_monthly_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change the dataset loaded from SQL as preferred. 
		
	save gdp_releases, replace

	
	odbc load, exec("select * from e_gdp_monthly_releases_seasonal_dummies") dsn("gdp_revisions_datasets") lowercase sqlshow clear // seasonal dummies specified for errors. 
		
	save gdp_bench_releases_e, replace
	
	
	odbc load, exec("select * from r_gdp_monthly_releases_seasonal_dummies") dsn("gdp_revisions_datasets") lowercase sqlshow clear // seasonal dummies specified for revisions. 
		
	save gdp_bench_releases_r, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(GDP releases)
	-----------------------*/

	
	use gdp_releases, clear

	if 1 == 1 {	
		
		* Remove the current definitive value (most recent release) for each target period and sector		
		drop *_most_recent		
		
		* Remove columns for h > 12		
		ds *_release_*
		
		foreach var in `r(varlist)' {
			if regexm("`var'", "([0-9]+)$") { // Extract the number at the end of the variable name
				local num = regexs(1)  // Capture the number

				if real("`num'") > 12 { // Drop if number > 12
					drop `var'
				}
			}
		}				
				
		* Format the date variable
		replace vintages_date = mofd(dofc(vintages_date))
		format vintages_date %tm
		
		* Set vintages_date as first column		
		order vintages_date
				
		* Sort by vintages_date		
		sort vintages_date
		
		* Keep obs in specific date range		
		keep if vintages_date > tm(2000m12) & vintages_date < tm(2023m11)	
	
	save gdp_releases_cleaned, replace
	}
	
	
	
	/*----------------------
	Compute revisions (r)
	and errors (e)
	-----------------------*/
	
	if 1 == 1 {	
	use gdp_releases_cleaned, clear
		
		* Generate revisions for each horizon
		forval i = 2/12 {
			gen r_`i'_gdp = gdp_release_`i' - gdp_release_`=`i'-1'
		}
			
		* Generate errors for each horizon
		forval i = 1/11 {
			gen e_`i'_gdp = gdp_release_12 - gdp_release_`i'
		}
			
	save gdp_releases_r_e, replace	
	}
	
	
	
	/*----------------------
	Clean benchmark data and
	compute logic-based indicators
	(for both errors and revisions)
	-----------------------*/

foreach suffix in e r {

	// Step 1: Clean benchmark seasonal dummies dataset

	use gdp_bench_releases_`suffix', clear

		* Keep only GDP release variables
		keep vintages_date gdp_release_*
				
		* Drop horizons h > 12
		ds gdp_release_*
		foreach var in `r(varlist)' {
			if regexm("`var'", "gdp_release_([0-9]+)") {
				local num = regexs(1)
				if real("`num'") > 12 {
					drop `var'
				}
			}
		}

		* Add the suffix "dummy" in each column
		foreach var of varlist * {
			if "`var'" != "vintages_date" {
				rename `var' `var'_dummy
			}
		}
		
		* Convert variables from double to int (except vintages_date)
		ds vintages_date, not
		recast int `r(varlist)', force

		* Format and sort time variable
		replace vintages_date = mofd(dofc(vintages_date))
		format vintages_date %tm
		order vintages_date
		sort vintages_date

		* Keep obs in specific date range
		keep if vintages_date > tm(2000m12) & vintages_date < tm(2023m11)

	save gdp_bench_releases_cleaned_`suffix', replace


	// Step 2: Compute revision logic indicator (r_i = 1 if one revision occurred)

	use gdp_bench_releases_cleaned_`suffix', clear

		* Generate dummy revisions for horizons 2 to 12
		forvalues i = 2/12 {
			gen r_`i'_gdp_dummy = (gdp_release_`i'_dummy + gdp_release_`=`i'-1'_dummy == 1)
		}

		* Keep only indicators and date
		keep vintages_date r_*_gdp_dummy

	save gdp_bench_`suffix', replace
}
	
	
	/*----------------------
	Merge benchmark and non-benchmark
	datasets (errors and revisions)
	-----------------------*/
	
foreach suffix in e r {
	use gdp_releases_r_e, clear

		merge 1:1 vintages_date using gdp_bench_`suffix'
		drop _merge

	save time_series_merged_`suffix', replace
}

	