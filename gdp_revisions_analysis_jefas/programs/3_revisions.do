/********************
Regressions to assess rationality on revisions
***

	Author
	---------------------
	Jason (for any issues email to jj.cruza@up.edu.pe)
	*********************/

	*** Program: revisions.do
	** 	First Created: 07/11/25
	** 	Last Updated:  09/09/25	
		
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
		
	di `"Please, enter your path for storing the (in/out)puts of this do-file in the COMMAND WINDOW and press ENTER."'  _request(path)
	
	cd "$path"
		
			
			
	/*----------------------
	Setting folders to save outputs
	------------------------*/

	shell mkdir "raw_data"		// Creating raw data folder.
	shell mkdir "input_data"	// Creating input data folder.
	shell mkdir "output" 		// Creating output folder.
*	shell mkdir "output/graphs" // Creating output charts folder.
	shell mkdir "output/tables" // Creating output tables folder.
			
		
	* Set as global vars
	
	global raw_data "raw_data"				// Use to raw data.
	global input_data "input_data"			// Use to import data.
*	global output_graphs "output/graphs"	// Use to export charts.
	global output_tables "output/tables"	// Use to export tables.
		
			

	/*----------------------
	Time Series Analysis
	-----------------------*/
	
	use "$input_data/r_gdp_revisions_ts", clear
	
		* Set time-variable
		tsset target_period, monthly
				
		* Keep common observations
		** Set common information using regression for the model with the least observations to keep if !missing(residuals)
		qui {
			newey r_12 L1.r_12 r_11, lag(6) force
			predict residuals_aux, resid  // Generate the regression residuals.
		}
		keep if !missing(residuals_aux)  // Keep only the observations where the residuals are not missing.
		qui drop residuals_aux
				
		* Generate explanatory vars (only once per loop)
		gen r_lag_t  	= .
		gen r_lag_h  	= .
		gen D_h 		= .
		gen Dr_lag_t	= .
		gen Dr_lag_h	= .
		gen r_1 		= .
		gen bench_r_1 	= .
				
		forval h = 2/12 {			
			capture confirm variable r_`h'
			
			if !_rc {				
				replace r_lag_t 	= L1.r_`h'
				replace r_lag_h 	= r_`=`h'-1'
				replace D_h 		= bench_r_`h'
				replace Dr_lag_t 	= bench_r_`h'*L1.r_`h'
				replace Dr_lag_h 	= bench_r_`h'*r_`=`h'-1'
						
				capture {			
					quietly count if !missing(r_`h')
					if r(N) < 5 continue  // Skip if there are less than 5 observations
					
					* Unbiasedness
					newey r_`h', lag(6) force					
					eststo r_bias_`h'
					
					* Autocorrelation
					newey r_`h' r_lag_t, lag(6) force	
					eststo r_auto_`h'
			
					* Cross-h correlation
					newey r_`h' r_lag_h, lag(6) force	
					eststo r_cros_`h'
					
					* Omnibus
					newey r_`h' r_lag_h r_lag_t, lag(6) force	
					eststo r_omni_`h'
					
					* Omnibus with benchmark revisions
					newey r_`h' r_lag_t r_lag_h D_h Dr_lag_t Dr_lag_h, lag(6) force
					eststo r_bench_omni_`h'
				}				
			}			
		}

		
		
	/*----------------------
	Results
	-----------------------*/
	
	cd "$path"
	cd "$output_tables"
	
	* Screen results
	noisily {
	esttab r_bias_*, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) 				
	esttab r_auto_*, order(_cons r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab r_cros_*, order(_cons r_lag_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab r_omni_*, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
	esttab r_bench_omni_*, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
	}

	* Exportable results (.txt)
	esttab r_bias_* using revisions.txt, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) replace				
	esttab r_auto_* using revisions.txt, order(_cons r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	esttab r_cros_* using revisions.txt, order(_cons r_lag_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	esttab r_omni_* using revisions.txt, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	noisily esttab r_bench_omni_* using revisions.txt, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) append

	* Exportable results (.xls)
	estout r_bias_* using revisions.xls, cells(b(fmt(4)) t(fmt(4) abs))	stats(N) replace				
	estout r_auto_* using revisions.xls, order(_cons r_lag_t) cells(b(fmt(4)) t(fmt(4) abs))	stats(N) append
	estout r_cros_* using revisions.xls, order(_cons r_lag_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	estout r_omni_* using revisions.xls, order(_cons r_lag_h r_lag_t) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	noisily estout r_bench_omni_* using revisions.xls, order(_cons r_lag_h r_lag_t) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	
	cd "$path"

	