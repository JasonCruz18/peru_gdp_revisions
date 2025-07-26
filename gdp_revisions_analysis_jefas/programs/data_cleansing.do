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
		
	save e_gdp_bench_releases, replace
	
	
	odbc load, exec("select * from r_gdp_monthly_releases_seasonal_dummies") dsn("gdp_revisions_datasets") lowercase sqlshow clear // seasonal dummies specified for revisions. 
		
	save r_gdp_bench_releases, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	-----------------------*/

	
	use gdp_releases, clear

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
	
		* Rename vars
		rename vintages_date target_period
		
		forvalue h = 1/12 {
			rename gdp_release_`h' y_`h' 
		}	 
				
		* Label vars
		label variable target_period "Monthly Targed Period"
		
		forvalue h = 1/12{
			label variable y_`h' "`h'-th GDP Release"
		}
		
		* Format the date variable
		replace target_period = mofd(dofc(target_period))
		format target_period %tm
		
		* Order and	sort by vintages_date
		order target_period		
		sort target_period
		
		* Keep obs in specific date range		
		keep if target_period > tm(2000m12) & target_period < tm(2023m11)	

		
		
		/*......................
		Compute revisions (r)
		and errors (e)
		........................*/
			
		* Generate revisions for each horizon
		forval h = 2/12 {
			gen r_`h' = y_`h' - y_`=`h'-1'
		}
		
		* Generate errors for each horizon
		forval h = 1/11 {
			gen e_`h' = y_12 - y_`h' // Assumed y_12 as the true
		}

		* Label revisions
		forvalue h = 2/12{
			label variable r_`h' "`=`h'-1'-th GDP Revision"
		}
		
		forvalue h = 1/11{
			label variable e_`h' "`h'-th GDP Error"
		}
		
		
	save gdp_releases_cleaned, replace
	
	
	
	/*----------------------
	Clean benchmark data and
	compute logic-based
	revisions dummies
	-----------------------*/
	
	
	foreach logic in e r {

	use `logic'_gdp_bench_releases, clear
	
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
	
		* Rename vars
		rename vintages_date target_period
		
		forvalue h = 1/12 {
			rename gdp_release_`h' y_`h' 
		}
			
		* Add the suffix "dummy" in each column
		foreach var of varlist * {
			if "`var'" != "target_period" {
				rename `var' bench_`var'
			}
		}
		
		* Convert variables from double to int (except vintages_date)
		ds target_period, not
		recast int `r(varlist)', force
		
		* Label vars
		label variable target_period "Monthly Targed Period"
		
		forvalue h = 1/12{
			label variable bench_y_`h' "`h'-th GDP Benchmark Release"
		}

		* Format the date variable
		replace target_period = mofd(dofc(target_period))
		format target_period %tm
		
		* Order and	sort by vintages_date
		order target_period
		sort target_period

		* Keep obs in specific date range
		keep if target_period > tm(2000m12) & target_period < tm(2023m11)
			
		
			
		/*......................
		Compute revision dummy logic
		(bench_r_h = 1 if one bench
		revision occurred)
		........................*/
		
		* Generate benchmark revisions for horizons 2 to 12
		forvalues h = 2/12 {
			gen bench_r_`h' = (bench_y_`h' + bench_y_`=`h'-1' == 1)
		}
		
		* Label benchmark revisions
		forvalue h = 2/12{
			label variable bench_r_`h' "`=`h'-1'-th Benchmark GDP Revision"
		}
		
		* Keep only bench revisions and target period
		keep target_period bench_y_* bench_r_*

		
	save `logic'_gdp_bench_releases_cleaned, replace
		
	}
	
	
	
	/*----------------------
	Merge benchmark and
	non-benchmark datasets
	(releases, revisions and
	errors)
	-----------------------*/
	
	
	foreach logic in e r {
	
	use gdp_releases_cleaned, clear

		merge 1:1 target_period using `logic'_gdp_bench_releases_cleaned
		drop _merge

	save `logic'_gdp_revisions_ts, replace
	
	}

	

	/*----------------------
	Build panel data
	-----------------------*/

	
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

