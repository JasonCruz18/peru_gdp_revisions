/********************
Revisions Regressions
***

	Author
	---------------------
	D & J
	*********************/

	*** Program: errors.do
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
				
	//log using jefas_encompassing.txt, text replace // Opens a log file and replaces it if it exists.

	
	
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
	Time Series Analysis
	-----------------------*/


	cd "$input_data"	
	use e_gdp_revisions_ts, clear
			
		* Keep common observations
		** Set common information using regression for the model with the least observations to keep if !missing(residuals)
		qui {
		tsset target_period, monthly
		newey e_11 y_11, lag(6) force
		predict residuals_aux, resid  // Generate the regression residuals.
		}
		keep if !missing(residuals_aux)  // Keep only the observations where the residuals are not missing.
		qui drop residuals_aux
			
		* Loop through variables r_`i'_gdp where `i' ranges from 3 to 12
		
		gen y_h    = .
		gen r_h    = .
		gen bench_r_h = .
		gen r_lag = .
		gen r_1 = .
		gen bench_r_1 = .
		gen e_lag  = .

			
		forval h = 1/11 {			
		capture confirm variable r_`h'

		if !_rc {
			replace y_h   = y_`h'
			replace r_h   = r_`h'
			replace bench_r_h = bench_r_`h'
			replace r_lag = L1.r_`h'
			replace e_lag = L1.e_`h'
					
			capture {			
				quietly count if !missing(e_`h')
				if r(N) < 5 continue  // Skip if there are less than 5 observations
				
				* Set time-variable
				tsset target_period, monthly
						
				* Unbiasedness		
				newey e_`h', lag(6) force					
				eststo e_bias_`h'
				
				* Mincer-Zarnowitz
				newey e_`h' y_h, lag(6) force	
				eststo e_mz_`h'

				* Encompassing
				newey e_`h' r_h, lag(6) force	
				eststo e_enco_`h'
				
				* Augmented Mincer-Zarnowitz
				newey e_`h' y_h r_h, lag(6) force	
				eststo e_amz_`h'	
				
				* Omnibus
				newey e_`h' y_h r_h r_lag, lag(6) force	
				eststo e_omni_`h'
				
				* Omnibus with benchmark revisions
				newey e_`h' y_h r_lag c.r_h##i.bench_r_h, lag(6) force	
				eststo e_bench_omni_`h'
				
				* Forecasting
				newey e_`h' y_h r_h r_lag e_lag, lag(6) force	
				eststo e_fore_`h'
			}				
		}			
		}
		

	cd "$path"
	cd "$output_tables"

	* Resultados
	esttab e_bias_* using errors.txt, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) replace				
	esttab e_mz_* using errors.txt, order(_cons y_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	esttab e_enco_* using errors.txt, order(_cons r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	esttab e_amz* using errors.txt, order(_cons y_h r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	esttab e_omni_* using errors.txt, order(_cons y_h r_h r_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	esttab e_bench_omni_* using errors.txt, drop(0.*) order(_cons y_h r_h r_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
	noisily esttab e_fore_* using errors.txt, order(_cons y_h r_h r_lag e_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append

	* Resultados en pantalla 
	noisily {
	esttab e_bias_* , se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) 				
	esttab e_mz_* , order(_cons y_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab e_enco_* , order(_cons r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab e_amz_*, order(_cons y_h r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N)  
	esttab e_omni_* , order(_cons y_h r_h r_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab e_bench_omni_* , drop(0.*) order(_cons y_h r_h r_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	esttab e_fore_* , order(_cons y_h r_h r_lag e_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
	}

	* Resultados
	estout e_bias_* using errors.xls, cells(b(fmt(4)) t(fmt(4) abs))	stats(N) replace				
	estout e_mz_* using errors.xls, order(_cons y_h) cells(b(fmt(4)) t(fmt(4) abs))	stats(N) append
	estout e_enco_* using errors.xls, order(_cons r_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	estout e_amz* using errors.xls, order(_cons y_h r_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	estout e_omni* using errors.xls, order(_cons y_h r_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	estout e_bench_omni* using errors.xls, drop(0.*) order(_cons y_h r_h r_lag) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
	noisily estout e_fore_* using errors.xls, order(_cons y_h r_h r_lag e_lag) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append

	cd "$path"
	
	
	
	/*----------------------
	Pooled Analysis
	-----------------------*/
	
	
	cd "$path"
	cd "$input_data"

	use e_gdp_revisions_panel, clear

		* Lag of the the revisions		
		xtset target_period horizon
				
		* Loop through variables r_`i'_gdp where `i' ranges from 3 to 12
		
		capture {			
			quietly count if !missing(r)
			if r(N) < 5 continue  // Skip if there are less than 5 observations
					
			xtreg e r L1.r, fe vce(cluster target_period)
			eststo e_omni_pooled_fe
			
			xtreg e r L1.r, re vce(cluster target_period)
			eststo e_omni_pooled_re
			
			xtreg e c.r##bench_r c.L1.r##i.L1.bench_r, fe vce(cluster target_period)
			eststo e_bench_omni_pooled_fe
			
			xtreg e c.r##bench_r c.L1.r##i.L1.bench_r, re vce(cluster target_period)
			eststo e_bench_omni_pooled_re
			
		}
		
		
	cd "$path"
	cd "$output_tables"

		* Resultados
		esttab e_omni_pooled_fe using errors_pooled.txt, order(_cons r L.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) replace
		esttab e_omni_pooled_re using errors_pooled.txt, order(_cons r L.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
		esttab e_bench_omni_pooled_fe using errors_pooled.txt, drop(0*) order(_cons r L.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
		noisily esttab e_bench_omni_pooled_re using errors_pooled.txt,drop(0*) order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) append

		* Resultados en pantalla 
		noisily { 
		esttab e_omni_pooled_fe, order(_cons r L.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		esttab e_omni_pooled_re, order(_cons r L.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		esttab e_bench_omni_pooled_fe, drop(0*) order(_cons r L.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		esttab e_bench_omni_pooled_re, drop(0*) order(_cons r L.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		}

		* Resultados
		estout e_omni_pooled_fe using errors_pooled.xls, order(_cons r L.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) replace
		estout e_omni_pooled_re using errors_pooled.xls, order(_cons r L.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
		estout e_bench_omni_pooled_fe using errors_pooled.xls, drop(0b*) order(_cons r L.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
		noisily estout e_bench_omni_pooled_re using errors_pooled.xls,drop(0b*) order(_cons r L.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append

	cd "$path"
	
	