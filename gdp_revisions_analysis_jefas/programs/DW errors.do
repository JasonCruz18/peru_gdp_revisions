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
			
//log using jefas_encompassing.txt, text replace // Opens a log file and replaces it if it exists.

/*----------------------
Defining workspace path
------------------------*/
global path "G:\Mi unidad\Proyecto_Revisiones\6. Programs"
cd "$path"

global tables_folder "output/tables"	// Use to export tables.
global data_folder "input_data"		// Use to export .dta.
	
		
		
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

cd "input_data"	
	
use r_e_gdp_releases, clear

		
		
* Lag of the the revisions		
tsset vintages_date, monthly
		
foreach i of numlist 2/12 {
	gen r_`i'_gdp_lagged_1 = L1.r_`i'_gdp
}
		
* Keep common observations
** Set common information using regression for the model with the least observations to keep if !missing(residuals)
qui {
	tsset vintages_date
	newey e_11_gdp gdp_release_11, lag(6) force
	predict residuals_aux, resid  // Generate the regression residuals.
}
keep if !missing(residuals_aux)  // Keep only the observations where the residuals are not missing.
qui drop residuals_aux
		
* Loop through variables r_`i'_gdp where `i' ranges from 3 to 12
gen r_h    = .
gen r_lag = .
gen e_lag  = .
gen y_h    = .
gen r_1_gdp = .

		
forval i = 1/11 {			
	capture confirm variable r_`i'_gdp
	
	if !_rc {
		replace y_h   = gdp_release_`i'
		replace r_h   = r_`i'_gdp
		replace r_lag = L1.r_`i'_gdp
		replace e_lag = L1.e_`i'_gdp
				
		capture {			
			quietly count if !missing(e_`i'_gdp)
			if r(N) < 5 continue  // Skip if there are less than 5 observations
					
			* Unbiasedness		
			newey e_`i'_gdp, lag(6) force					
			eststo e_Bias`i'
			
			* Mincer-Zarnowitz
			newey e_`i'_gdp y_h, lag(6) force	
			eststo e_MiZa`i'
	
			* Encompassing
			newey e_`i'_gdp r_h, lag(6) force	
			eststo e_Enco`i'
			
			* Augmented Mincer-Zarnowitz
			newey e_`i'_gdp y_h r_h, lag(6) force	
			eststo e_AMiZa`i'	
			
			* Omnibus
			newey e_`i'_gdp y_h r_h r_lag, lag(6) force	
			eststo e_Omni`i'	
			
			* Forecasting
			newey e_`i'_gdp y_h r_h r_lag e_lag, lag(6) force	
			eststo e_Fore`i'
		}				
	}			
}


cd "$path"
cd "$tables_folder"

* Resultados
esttab e_Bias* using R_errors.txt, se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) replace				
esttab e_MiZa* using R_errors.txt, order(_cons y_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
esttab e_Enco* using R_errors.txt, order(_cons r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
esttab e_AMiZa* using R_errors.txt, order(_cons y_h r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
esttab e_Omni* using R_errors.txt, order(_cons y_h r_h r_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append
noisily esttab e_Fore* using R_errors.txt, order(_cons y_h r_h r_lag e_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) append

* Resultados en pantalla 
noisily {
esttab e_Bias* , se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps	scalar(N) 				
esttab e_MiZa* , order(_cons y_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
esttab e_Enco* , order(_cons r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
esttab e_AMiZa*, order(_cons y_h r_h) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N)  
esttab e_Omni* , order(_cons y_h r_h r_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
esttab e_Fore* , order(_cons y_h r_h r_lag) se b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) compress nogaps scalar(N) 
}

* Resultados
estout e_Bias* using R_errors.xls, cells(b(fmt(4)) t(fmt(4) abs))	stats(N) replace				
estout e_MiZa* using R_errors.xls, order(_cons y_h) cells(b(fmt(4)) t(fmt(4) abs))	stats(N) append
estout e_Enco* using R_errors.xls, order(_cons r_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
*estout e_AMiZa* using R_errors.xls, order(_cons y_h r_h) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append
noisily estout e_Omni* using R_errors.xls, order(_cons y_h r_h r_lag) cells(b(fmt(4)) t(fmt(4) abs)) stats(N) append

cd "$path"