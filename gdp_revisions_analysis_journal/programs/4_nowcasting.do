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

*	di `"Please, enter your path for storing the (in/out)puts of this do-file in the COMMAND WINDOW and press ENTER."'  _request(path)
	
*	cd "$path"
		
	
	
	/*----------------------
	Setting folders to store (in/out)puts
	------------------------*/
	
*	shell mkdir "raw_data"		// Creating raw data folder.
*	shell mkdir "input_data"	// Creating input data folder.
*	shell mkdir "output" 		// Creating output folder.
*	shell mkdir "output/graphs" // Creating output charts folder.
*	shell mkdir "output/tables" // Creating output tables folder.
			
		
	* Set as global vars
	
*	global raw_data "raw_data"				// Use to raw data.
*	global input_data "input_data"			// Use to import data.
*	global output_graphs "output/graphs"	// Use to export charts.
*	global output_tables "output/tables"	// Use to export tables.

	

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
			newey e_1 y_1, lag(6) force
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
	Nowcast Evaluation
	----------------------*/

	use "$input_data/fitted_vals", clear
	
	
		* Set time-variable
		tsset target_period, monthly
		
		* Keep common observations
		** Set common information using regression for the model with the least observations to keep if !missing(residuals)
		qui {
			newey e_1 y_1, lag(6) force
			predict residuals_aux, resid
		}
		keep if !missing(residuals_aux)
		drop residuals_aux
		

		/*++++++++++++++++++++
		Relative RMSE
		++++++++++++++++++++*/

		* Create an empty matrix to hold RMSE results
		matrix rmse_results = J(1, 11, .)

		forvalues h = 1/11 {
			gen sq_now = (e_now_`h')^2
			gen sq_bench = (e_`h')^2
			quietly summarize sq_now
			local rmse_now = sqrt(r(mean))
			quietly summarize sq_bench
			local rmse_bench = sqrt(r(mean))
			drop sq_now sq_bench
			local rmse_rel = `rmse_now' / `rmse_bench'
			
			* Store RMSE results in the matrix
			matrix rmse_results[1, `h'] = `rmse_rel' * 100  // Scaled by 100
		}
		

		/*++++++++++++++++++++
		DM test (MSE based)
		++++++++++++++++++++*/

		* Create an empty matrix to hold DM t-stat results
		matrix dm_results = J(1, 11, .)

		forvalues h = 1/11 {
			gen d_`h'_dm = (e_now_`h')^2 - (e_`h')^2
			newey d_`h'_dm, lag(6) force
			scalar dm_stat_`h' = _b[_cons] / _se[_cons]
			
			* Store DM test results in the matrix
			matrix dm_results[1, `h'] = dm_stat_`h'
		}

		
		/*++++++++++++++++++++
		DM test (MAE based)
		++++++++++++++++++++*/
		
		* Create an empty matrix to hold MAE-based DM t-stat results
		matrix dm_mae_results = J(1, 11, .)

		forvalues h = 1/11 {
			gen d_`h'_dm_mae = abs(e_now_`h') - abs(e_`h')
			newey d_`h'_dm_mae, lag(6) force
			scalar dm_mae_stat_`h' = _b[_cons] / _se[_cons]
			
			* Store MAE-based DM test results in the matrix
			matrix dm_mae_results[1, `h'] = dm_mae_stat_`h'
		}
		
		
		/*++++++++++++++++++++
		Encompassing test (->)
		++++++++++++++++++++*/

		* Create an empty matrix to hold Encompassing t-stat results
		matrix encom_results = J(1, 11, .)

		forvalues h = 1/11 {
			gen d_`h'_encom_1 = e_`h' - e_now_`h'  // Renamed to d_`h'_encom_1 to avoid conflict
			newey e_`h' d_`h'_encom_1, lag(6) force
			scalar tstat_`h' = _b[d_`h'_encom_1] / _se[d_`h'_encom_1]
			
			* Store Encompassing t-stat results in the matrix
			matrix encom_results[1, `h'] = tstat_`h'
		}
		
		
		/*++++++++++++++++++++
		Encompassing test (<-)
		++++++++++++++++++++*/

		* Create an empty matrix to hold Encompassing t-stat results
		matrix encom_results_2 = J(1, 11, .)

		forvalues h = 1/11 {
			gen d_`h'_encom_2 = e_now_`h' - e_`h'  // Renamed to d_`h'_encom_2 to avoid conflict
			newey e_now_`h' d_`h'_encom_2, lag(6) force
			scalar tstat_2_`h' = _b[d_`h'_encom_2] / _se[d_`h'_encom_2]
			
			* Store Encompassing t-stat results in the matrix
			matrix encom_results_2[1, `h'] = tstat_2_`h'
		}

		
		
	/*----------------------
	Export results
	----------------------*/
	
	* Combine RMSE, DM t-stat, and encompassing t-stat into one matrix
	matrix results = rmse_results \ dm_results \ dm_mae_results \ encom_results \ encom_results_2

	* Export directly from matrix to Excel (.xls)
	putexcel set "$output_tables/ex-post_nwc.xls", sheet("ex-post_nwc") replace

	putexcel A1 = matrix(results), names

	* Label rows and columns in Excel
	putexcel A1 = ("") B1 = ("1") C1 = ("2") D1 = ("3") E1 = ("4") F1 = ("5") /// 
		G1 = ("6") H1 = ("7") I1 = ("8") J1 = ("9") K1 = ("10") L1 = ("11")
		
	putexcel A2 = ("RMSE") B2 = results[1,1] C2 = results[1,2] D2 = results[1,3] E2 = results[1,4] /// 
		F2 = results[1,5] G2 = results[1,6] H2 = results[1,7] I2 = results[1,8] J2 = results[1,9] K2 = results[1,10] L2 = results[1,11]

	putexcel A3 = ("DM (MSE)") B3 = results[2,1] C3 = results[2,2] D3 = results[2,3] E3 = results[2,4] /// 
		F3 = results[2,5] G3 = results[2,6] H3 = results[2,7] I3 = results[2,8] J3 = results[2,9] K3 = results[2,10] L3 = results[2,11]

	putexcel A4 = ("DM (MAE)") B4 = results[3,1] C4 = results[3,2] D4 = results[3,3] E4 = results[3,4] /// 
		F4 = results[3,5] G4 = results[3,6] H4 = results[3,7] I4 = results[3,8] J4 = results[3,9] K4 = results[3,10] L4 = results[3,11]

	putexcel A5 = ("Encompassing (->)") B5 = results[4,1] C5 = results[4,2] D5 = results[4,3] E5 = results[4,4] /// 
		F5 = results[4,5] G5 = results[4,6] H5 = results[4,7] I5 = results[4,8] J5 = results[4,9] K5 = results[4,10] L5 = results[4,11]

	putexcel A6 = ("Encompassing (<-)") B6 = results[5,1] C6 = results[5,2] D6 = results[5,3] E6 = results[5,4] /// 
		F6 = results[5,5] G6 = results[5,6] H6 = results[5,7] I6 = results[5,8] J6 = results[5,9] K6 = results[5,10] L6 = results[5,11]

