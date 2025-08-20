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
		
			

	/*----------------------
	Time Series Analysis
	-----------------------*/


	cd "$input_data"		
	use r_gdp_revisions_ts, clear
				
		* Set common information using regression for the model with the least observations to keep if !missing(residuals)
		qui {
			tsset target_period, monthly
			newey r_12 L1.r_12 r_11, lag(6) force
			predict residuals_aux, resid  // Generate the regression residuals.
		}
		keep if !missing(residuals_aux)  // Keep only the observations where the residuals are not missing.
		qui drop residuals_aux
				
		* Loop through variables r_`i'_gdp where `i' ranges from 3 to 12
		gen r_lag_t  = .
		gen r_lag_h  = .
		gen bench_r_lag_h = .
		gen bench_r_lag_t = .
		gen r_1 = .
		gen bench_r_1 = .
				
		forval h = 2/12 {			
			capture confirm variable r_`h'
			
			if !_rc {				
				replace r_lag_t = L1.r_`h'
				replace r_lag_h = r_`=`h'-1'
				replace bench_r_lag_h = bench_r_`=`h'-1'
				replace bench_r_lag_t = L1.bench_r_`h'
						
				capture {			
					quietly count if !missing(r_`h')
					if r(N) < 5 continue  // Skip if there are less than 5 observations
					* Set time-variable
					tsset target_period, monthly
					
					* Run regressions
					newey r_`h', lag(6) force					
					eststo r_bias_`h'
					
					newey r_`h' r_lag_t, lag(6) force	
					eststo r_auto_`h'
			
					newey r_`h' r_lag_h, lag(6) force	
					eststo r_cros_`h'
					
					newey r_`h' r_lag_h r_lag_t, lag(6) force	
					eststo r_omni_`h'
					
					newey r_`h' c.r_lag_t##i.bench_r_lag_t c.r_lag_h##i.bench_r_lag_h, lag(6) force
					eststo r_bench_omni_`h'
				}				
			}			
		}

		
		
		newey r_4 c.L1.r_4##i.L1.bench_r_4 c.r_3##i.bench_r_3, lag(6) force

	cd "$path"
	cd "$output_tables"

		* Resultados
		esttab r_bias_* using revisions.txt, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) replace				
		esttab r_auto_* using revisions.txt, order(_cons r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
		esttab r_cros_* using revisions.txt, order(_cons r_lag_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
		esttab r_omni_* using revisions.txt, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
		noisily esttab r_bench_omni_* using revisions.txt, drop(0.*) order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) append

		* Resultados en pantalla 
		noisily {
		esttab r_bias_*, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) 				
		esttab r_auto_*, order(_cons r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
		esttab r_cros_*, order(_cons r_lag_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
		esttab r_omni_*, order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		esttab r_bench_omni_*, drop(0.*) order(_cons r_lag_h r_lag_t) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		}

		* Resultados
		estout r_bias_* using revisions.xls, cells(b(fmt(4)) t(fmt(4) abs))	stats(N) replace				
		estout r_auto_* using revisions.xls, order(_cons r_lag_t) cells(b(fmt(4)) t(fmt(4) abs))	stats(N) append
		estout r_cros_* using revisions.xls, order(_cons r_lag_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
		estout r_omni_* using revisions.xls, order(_cons r_lag_h r_lag_t) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
		noisily estout r_bench_omni_* using revisions.xls, drop(0.*) order(_cons r_lag_h r_lag_t) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append

		
		
	/*----------------------
	Pooled Analysis
	-----------------------*/
	
	
	cd "$path"
	cd "$input_data"

	use r_gdp_revisions_panel, clear

		* Lag of the the revisions		
		xtset target_period horizon
				
		* Loop through variables r_`i'_gdp where `i' ranges from 3 to 12
		
		capture {			
			quietly count if !missing(r)
			if r(N) < 5 continue  // Skip if there are less than 5 observations
					
			xtreg r L1.r L2.r, fe vce(cluster target_period)
			eststo r_omni_pooled_fe
			
			xtreg r L1.r L2.r, re vce(cluster target_period)
			eststo r_omni_pooled_re
			
			xtreg r c.L1.r##L1.bench_r c.L2.r##L2.bench_r, fe vce(cluster target_period)
			eststo r_bench_omni_pooled_fe
			
			xtreg r c.L1.r##L1.bench_r c.L2.r##L2.bench_r, re vce(cluster target_period)
			eststo r_bench_omni_pooled_re
			
		}
		

	cd "$path"
	cd "$output_tables"

		* Resultados
		esttab r_omni_pooled_fe using revisions_pooled.txt, order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) replace
		esttab r_omni_pooled_re using revisions_pooled.txt, order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
		esttab r_bench_omni_pooled_fe using revisions_pooled.txt, drop(0b*) order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
		noisily esttab r_bench_omni_pooled_re using revisions_pooled.txt,drop(0b*) order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) append

		* Resultados en pantalla 
		noisily { 
		esttab r_omni_pooled_fe, order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		esttab r_omni_pooled_re, order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		esttab r_bench_omni_pooled_fe, drop(0b*) order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		esttab r_bench_omni_pooled_re, drop(0b*) order(_cons L.r L2.r) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N r2) 
		}

		* Resultados
		estout r_omni_pooled_fe using revisions_pooled.xls, order(_cons L.r L2.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) replace
		estout r_omni_pooled_re using revisions_pooled.xls, order(_cons L.r L2.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
		estout r_bench_omni_pooled_fe using revisions_pooled.xls, drop(0b*) order(_cons L.r L2.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
		noisily estout r_bench_omni_pooled_re using revisions_pooled.xls,drop(0b*) order(_cons L.r L2.r) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append

	cd "$path"

	