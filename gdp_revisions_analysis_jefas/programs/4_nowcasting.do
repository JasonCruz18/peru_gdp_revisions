/********************
Ex-post nowcasting exercise
***

	Author
	---------------------
	Jason (for any issues email to jj.cruza@up.edu.pe)
	*********************/

	*** Program: nowcasting.do
	** 	First Created: 08/11/25
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
				
	//log using jefas_encompassing.txt, text replace // Opens a log file and replaces it if it exists.

	
	
	/*----------------------
	Defining workspace path
	------------------------*/

	di `"Please, enter your path for storing the (in/out)puts of this do-file in the COMMAND WINDOW and press ENTER."'  _request(path)
	
	cd "$path"
		
	
	
	/*----------------------
	Setting folders to store (in/out)puts
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
	Clean-up at a glance
	-----------------------*/

	use "$input_data/e_gdp_revisions_ts", clear

		* Drop benchmark revision dummies since they are no longer needed
		drop bench_*



	/*----------------------
	Omnibus regressions
	-----------------------*/

		* Set time-variable
		tsset target_period, monthly
		
		* Keep common observations
		** Set common information using regression for the model with the least observations to keep if !missing(residuals)
		qui {
			newey e_1 y_1 L1.e_1, lag(6) force
			predict residuals_aux, resid
		}
		keep if !missing(residuals_aux)
		drop residuals_aux

		forvalues h = 1/11 {
			if `h' == 1 {
				newey e_`h' L1.e_`h' y_`h', lag(6) force
				matrix b = e(b)
				gen alpha_`h' = b[1, "_cons"]
				gen theta_`h' = b[1, "y_`h'"]
				gen delta_`h' = b[1, "L1.e_`h'"]
			}
			else if `h' == 2 {
				newey e_`h' L1.e_`h' y_`h' r_`h', lag(6) force
				matrix b = e(b)
				gen alpha_`h' = b[1, "_cons"]
				gen theta_`h' = b[1, "y_`h'"]
				gen delta_`h' = b[1, "L1.e_`h'"]
				gen gamma_`h' = b[1, "r_`h'"]
			}
			else {
				newey e_`h' L1.e_`h' y_`h' r_`h' L1.r_`h', lag(6) force
				matrix b = e(b)
				gen alpha_`h' = b[1, "_cons"]
				gen theta_`h' = b[1, "y_`h'"]
				gen delta_`h' = b[1, "L1.e_`h'"]
				gen gamma_`h' = b[1, "r_`h'"]
				gen rho_`h'   = b[1, "L1.r_`h'"]
			}
		}

		
	tempfile omnibus_coeffs
	save `omnibus_coeffs', replace



	/*----------------------
	EWS construction
	-----------------------*/

		tsfill, full // Ensure the whole range for target periods
	* 	local delta = 0.3

		forvalues h = 1/11 {
			gen Y_ews_`h' = .
			quietly replace Y_ews_`h' = y_`h' in 1
			forvalues t = 2/`=_N' {
				quietly replace Y_ews_`h' = delta_`h'*L1.Y_ews_`h' + y_`h' in `t' if !missing(y_`h') & !missing(L1.Y_ews_`h')
				quietly replace Y_ews_`h' = L1.Y_ews_`h' in `t' if missing(y_`h')
			}
		}

		forvalues h = 2/11 {
			gen R_ews_`h' = .
			quietly replace R_ews_`h' = r_`h' in 1
			forvalues t = 2/`=_N' {
				quietly replace R_ews_`h' = delta_`h'*L1.R_ews_`h' + r_`h' in `t' if !missing(r_`h') & !missing(L1.R_ews_`h')
				quietly replace R_ews_`h' = L1.R_ews_`h' in `t' if missing(r_`h')
			}
		}

		forvalues h = 3/11 {
			gen L1_R_ews_`h' = L1.R_ews_`h'
		}

		
	tempfile ews
	save `ews', replace



	/*----------------------
	Fitted values
	-----------------------*/

		forvalues h = 1/11 {
			gen e_hat_`h' = .
			if `h' == 1 {
				replace e_hat_`h' = (alpha_`h')/(1 - delta_`h') + theta_`h'*Y_ews_`h'
			}
			else if `h' == 2 {
				replace e_hat_`h' = (alpha_`h')/(1 - delta_`h') + theta_`h'*Y_ews_`h' + gamma_`h'*R_ews_`h'
			}
			else {
				replace e_hat_`h' = (alpha_`h')/(1 - delta_`h') + theta_`h'*Y_ews_`h' + gamma_`h'*R_ews_`h' + rho_`h'*L1_R_ews_`h'
			}
		}

		forvalues h = 1/11 {
			gen y_hat_`h' = y_`h' + e_hat_`h'
		}

		forvalues h = 1/11 {
			gen e_now_`h' = .
			replace e_now_`h' = y_12 - y_hat_`h'
		}


	save "$input_data/fitted_vals", replace

	

	/*----------------------
	Nowcast evaluation
	-----------------------*/

	use "$input_data/fitted_vals", clear
	
