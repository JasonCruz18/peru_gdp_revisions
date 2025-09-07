/********************
Data Clean-up
***

	Author
	---------------------
	Jason (for any issues email to jj.cruza@up.edu.pe)
	*********************/

	*** Program: data_cleansing.do
	** 	First Created: 07/11/25
	** 	Last Updated:  09/09/25	
		
***
** Just click on the "Run (do)" button, the code will do the rest for you.
***
	
	

	/*----------------------
	Initial setting
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
		
	di `"Please, enter your path for storing the (in/out)puts of this do-file in the COMMAND WINDOW and press ENTER."'  _request(path)
	
	cd "$path"
			
			
			
	/*----------------------
	Setting folders to store (in/out)puts
	------------------------*/
	
	shell mkdir "input"			// Creating input folder.
	shell mkdir "input/data"	// Creating input data folder.
	shell mkdir "output" 		// Creating output folder.
*	shell mkdir "output/graphs" // Creating output charts folder.
	shell mkdir "output/tables" // Creating output tables folder.
			
		
	* Set as global vars
		
	global input_data "input/data"			// Use to import data (gdp_releases.dta).
*	global output_graphs "output/graphs"	// Use to export charts.
	global output_tables "output/tables"	// Use to export tables.
	
	
	cd "$input_data"
			
			
			
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
	
*	import delimited using "e_gdp_monthly_releases.csv", clear // Uncomment this code if you were provided with csv datasets. 
		
	odbc load, exec("select * from e_gdp_monthly_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Error-specific releases
		
	save e_gdp_releases, replace
	
	
	odbc load, exec("select * from e_gdp_monthly_releases_seasonal_dummies") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Error-specific benchmark revision dummies
		
	save e_gdp_bench_releases, replace

	
	odbc load, exec("select * from r_gdp_monthly_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Revision-specific releases
		
	save r_gdp_releases, replace
	
	
	odbc load, exec("select * from r_gdp_monthly_releases_seasonal_dummies") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Revision-specific benchmark revision dummies
		
	save r_gdp_bench_releases, replace
	
	
	
	/*----------------------
	Clean releases data and
	compute revisions (r)
	and errors (e)
	-----------------------*/
	
	foreach suffix in e r {
	
	use `suffix'_gdp_releases, clear

	
		* Remove the old true value
		drop *_most_recent		
	
		* Remove releases for h > 12 (12-th release will be the new true value)		
		ds *_release_*
		
		foreach var in `r(varlist)' {
			if regexm("`var'", "([0-9]+)$") { // Extract the number at the end of the variable name
				local num = regexs(1)  	// Capture the number

				if real("`num'") > 12 { // Drop if number > 12
					drop `var'
				}
			}
		}	
	
		* Rename vars
		rename vintages_date target_period
		
		forvalue h = 1/12 {
			rename gdp_release_`h' y_`h' 
		}	 
				
		* Label vars
		label variable target_period "Monthly Target Period"
		
		
		label variable y_1 "1st GDP Release"
		label variable y_2 "2nd GDP Release"
		label variable y_2 "3rd GDP Release"
		
		forvalue h = 4/12{
			label variable y_`h' "`h'-th GDP Release"
		}
		
		* Format the date variable
		replace target_period = mofd(dofc(target_period))
		format target_period %tm
		
		* Order and	sort by target_period
		order target_period		
		sort target_period
		
		* Keep obs in specific date range		
		keep if inrange(target_period, tm(2001m1), tm(2023m10))

		
		/*++++++++++++++++++++++
		Compute r and e
		++++++++++++++++++++++++*/
			
		* Generate revisions for each horizon
		forval h = 2/12 {
			gen r_`h' = y_`h' - y_`=`h'-1'
		}
		
		* Generate errors for each horizon
		forval h = 1/11 {
			gen e_`h' = y_12 - y_`h' // Assumed y_12 as the true
		}

		* Label revisions
		label variable r_2 "1st GDP Revision"
		label variable r_3 "2nd GDP Revision"
		label variable r_4 "3rd GDP Revision"
		
		forvalue h = 5/12{
			label variable r_`h' "`=`h'-1'-th GDP Revision"
		}
		
		label variable e_1 "1st GDP Error"
		label variable e_2 "2nd GDP Error"
		label variable e_3 "3rd GDP Error"
		
		forvalue h = 4/11{
			label variable e_`h' "`h'-th GDP Error"
		}
		
		* Handle COVID observations
		** Replace values with missing for all numeric variables (except target_period) in the COVID window: 2020m3–2021m10
		foreach var of varlist _all {
			if "`var'" != "target_period" {
				capture confirm numeric variable `var'
				if !_rc {
					replace `var' = . if inrange(target_period, tm(2020m3), tm(2021m10))
				}
			}
		}
	
	 	** Optionally, instead of wiping the full window, we can restrict to just the aberrant COVID months (very large or short growth rates)
	
		*** Flag outlier months
	*	gen covid_outlier = inlist(target_period, tm(2020m4), tm(2020m5), tm(2021m4), tm(2021m5))

		*** Replace values with missing for those outlier months
	*	foreach var of varlist _all {
	*		if "`var'" != "target_period" {
	*			capture confirm numeric variable `var'
	*			if !_rc {
	*				replace `var' = . if covid_outlier == 1
	*			}
	*		}
	*	}

		
	save `suffix'_gdp_releases_cleaned, replace
	
	}
	
	
	
	/*----------------------
	Clean benchmark data and
	compute logic-based
	revisions dummies
	-----------------------*/
	
	foreach logic in e r { // Since datasets are revision- and error-specific, computing dummies for benchmark is revision- and error-logic-based  (see the data-building official documentation)

	use `logic'_gdp_bench_releases, clear
	
	
		* Remove the old true value
		drop *_most_recent		
	
		* Remove releases for h > 12 (12-th release will be the new true value)	
		ds *_release_*
		
		foreach var in `r(varlist)' {
			if regexm("`var'", "([0-9]+)$") { // Extract the number at the end of the variable name
				local num = regexs(1)  	// Capture the number

				if real("`num'") > 12 { // Drop if number > 12
					drop `var'
				}
			}
		}	
	
		* Rename vars
		rename vintages_date target_period
		
		forvalue h = 1/12 {
			rename gdp_release_`h' y_`h' 
		}
			
		* Add the suffix "bench" (dummy var) in each vars to differentiate with growth rates
		foreach var of varlist * {
			if "`var'" != "target_period" {
				rename `var' bench_`var'
			}
		}
		
		* Convert variables from double to byte (except target_period)
		ds target_period, not // List all variables except target_period
		recast byte `r(varlist)', force
		
		* Label vars
		label variable target_period "Monthly Targed Period"
		
		label variable bench_y_1 "1st GDP Benchmark Release"
		label variable bench_y_2 "2nd GDP Benchmark Release"
		label variable bench_y_3 "3rd GDP Benchmark Release"
		
		forvalue h = 4/12{
			label variable bench_y_`h' "`h'-th GDP Benchmark Release"
		}

		* Format the date variable
		replace target_period = mofd(dofc(target_period))
		format target_period %tm
		
		* Order and	sort by target_period
		order target_period
		sort target_period

		* Keep obs in specific date range
		keep if inrange(target_period, tm(2001m1), tm(2023m10))
			
			
		/*++++++++++++++++++++++
		Compute dummy for
		benchmark r and e
		++++++++++++++++++++++++*/
		
		* Generate benchmark revisions for horizons 2 to 12
		forvalues h = 2/12 {
			gen bench_r_`h' = .
			replace bench_r_`h' = (bench_y_`h' + bench_y_`=`h'-1' == 1) if !missing(bench_y_`h') & !missing(bench_y_`=`h'-1')
		}
		
		* Label benchmark revisions
		label variable bench_r_2 "1st Benchmark GDP Revision"
		label variable bench_r_3 "2nd Benchmark GDP Revision"
		label variable bench_r_4 "3rd Benchmark GDP Revision"
		
		forvalue h = 5/12{
			label variable bench_r_`h' "`=`h'-1'-th Benchmark GDP Revision"
		}
		
		* Keep only bench revisions and releases, and target period
		keep target_period bench_y_* bench_r_*

		* Handle COVID observations
		** Replace values with missing for all numeric variables (except target_period) in the COVID window: 2020m3–2021m10
		foreach var of varlist _all {
			if "`var'" != "target_period" {
				capture confirm numeric variable `var'
				if !_rc {
					replace `var' = . if inrange(target_period, tm(2020m3), tm(2021m10))
				}
			}
		}
	
	 	** Optionally, instead of wiping the full window, we can restrict to just the aberrant COVID months (very large or short growth rates)
	
		*** Flag outlier months
	*	gen covid_outlier = inlist(target_period, tm(2020m4), tm(2020m5), tm(2021m4), tm(2021m5))

		*** Replace values with missing for those outlier months
	*	foreach var of varlist _all {
	*		if "`var'" != "target_period" {
	*			capture confirm numeric variable `var'
	*			if !_rc {
	*				replace `var' = . if covid_outlier == 1
	*			}
	*		}
	*	}
		
		
	save `logic'_gdp_bench_releases_cleaned, replace
		
	}
	
	
	
	/*----------------------
	Merge benchmark and
	non-benchmark datasets
	(releases, r and e)
	-----------------------*/
	
	foreach suffix in e r {
	
	use `suffix'_gdp_releases_cleaned, clear
	

		merge 1:1 target_period using `suffix'_gdp_bench_releases_cleaned
		drop _merge

		
	save `suffix'_gdp_revisions_ts, replace
	
	}

	

	/*----------------------
	Build panel data
	(just in case)
	-----------------------*/
	
	/*
	foreach logic in e r {
	
	use `logic'_gdp_revisions_ts, clear	
	
	
		* Create a temporary identifier
		gen id = _n
		
		* Generate aux vars for having complete iteration logic	
		gen r_1 = . 		// does not really exist
		gen bench_r_1 = . 	// does not really exist
		gen e_12 = .		// does not really exist
		
		* Rename current vars names to have suitable names for reshaping data
		forval h = 1/12 {
			rename y_`h' y`h'
			rename r_`h' r`h'
			rename bench_r_`h' bench_r`h'
			rename e_`h' e`h'
		}
		
		* Reshape to long panel format
		reshape long y r bench_r e , i(id) j(horizon)
		
		* Drop temporary id and aux var
		drop id

		* Order, sort and label new vars
		order target_period horizon
		sort target_period horizon
		label variable y "GDP Release"
		label variable r "GDP Revision"
		label variable bench_r "GDP Benchmark Revision"
		label variable e "GDP Error"

		
	save `logic'_gdp_revisions_panel, replace
	
	}	

	*/
	
	