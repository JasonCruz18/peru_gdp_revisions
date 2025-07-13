/********************
Revisions Regressions
***

		Author
		---------------------
		D & J
		*********************/

		*** Program: revisions.do
		** 	First Created: 07/11/25
		** 	Last Updated:  07/12/25
			
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
		
			
			
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
		
		
*	odbc load, exec("select * from e_gdp_monthly_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change the dataset loaded from SQL as preferred. 
		
	
*	save gdp_releases, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(GDP releases)
	-----------------------*/

	
	cd "$input_data"	
	use gdp_releases, clear

		if 1 == 1 {	
				
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
				keep if vintages_date > tm(2000m12) & vintages_date < tm(2023m11)		
			
			save gdp_releases_cleaned, replace
		}
		
		
		
	/*----------------------
	Compute ongoing
	revisions (r)
	-----------------------*/
		
		
	if 1 == 1 {	
	use gdp_releases_cleaned, clear
		
		* Generate ongoing revisions for each horizon and sector
		forval i = 2/11 {
			gen r_`i'_gdp = gdp_release_`i' - gdp_release_`=`i'-1'
		}
			
		* Compute final revision (12th horizon)
		gen r_12_gdp = gdp_most_recent - gdp_release_11				

		
		save r_gdp_releases, replace
			
		forval i = 1/11 {
			gen e_`i'_gdp = gdp_most_recent - gdp_release_`i'
		}
			
		save r_e_gdp_releases, replace
		
	}
	
	
	
	**********************************	
		
		
	use r_e_gdp_releases, clear
	
			
	* Lag of the the revisions		
	tsset vintages_date, monthly
			
	foreach i of numlist 2/12 {
		gen r_`i'_gdp_lag_1 = L1.r_`i'_gdp
	}
			
	* Keep common observations
	** Set common information using regression for the model with the least observations to keep if !missing(residuals)
	qui {
		tsset vintages_date
		newey r_12_gdp r_12_gdp_lag_1, lag(6) force
		predict residuals_aux, resid  // Generate the regression residuals.
	}
	keep if !missing(residuals_aux)  // Keep only the observations where the residuals are not missing.
	qui drop residuals_aux
			
	* Loop through variables r_`i'_gdp where `i' ranges from 3 to 12
	gen r_lag_t  = .
	gen r_lag_h  = .
	gen r_1_gdp = .
			
	forval i = 2/12 {			
		capture confirm variable r_`i'_gdp
		
		if !_rc {				
			replace r_lag_t = r_`i'_gdp_lag_1
			replace r_lag_h = r_`=`i'-1'_gdp
					
			capture {			
				quietly count if !missing(r_`i'_gdp)
				if r(N) < 5 continue  // Skip if there are less than 5 observations
						
				newey r_`i'_gdp, lag(6) force					
				eststo r_bias_`i'
				
				newey r_`i'_gdp r_lag_t, lag(6) force	
				eststo r_auto_`i'
		
				newey r_`i'_gdp r_lag_h, lag(6) force	
				eststo r_cros_`i'
				
				newey r_`i'_gdp r_lag_h r_lag_t, lag(6) force	
				eststo r_omni_`i'					
			}				
		}			
	}

	
	cd "$path"
	cd "$output_tables"

	* Resultados
	esttab r_bias_* using revisions.txt, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) replace				
	esttab r_auto_* using revisions.txt, order(_cons r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	esttab r_cros_* using revisions.txt, order(_cons r_lag_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	noisily esttab r_omni_* using revisions.txt, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) append

	* Resultados en pantalla 
	noisily {
	esttab r_bias_*, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) 				
	esttab r_auto_*, order(_cons r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab r_cros_*, order(_cons r_lag_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab r_omni_*, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2)  
	}

	* Resultados
	estout r_bias_* using revisions.xls, cells(b(fmt(4)) t(fmt(4) abs))	stats(N) replace				
	estout r_auto_* using revisions.xls, order(_cons r_lag_t) cells(b(fmt(4)) t(fmt(4) abs))	stats(N) append
	estout r_cros_* using revisions.xls, order(_cons r_lag_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	noisily estout r_omni_* using revisions.xls, order(_cons r_lag_h r_lag_t) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append

	cd "$path"