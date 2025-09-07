/********************
Nowcasting GDP Revisions — EWMA
***

	Author
	---------------------
	D & J
	*********************/

	*** Program: nowcasting.do
	** 	First Created: 08/11/25
	** 	Last Updated:  08/25/25
		
***/


/*----------------------
Initial do-file setting
-----------------------*/

cls
clear all
version
set more off
cap set maxvar 12000
program drop _all
capture log close
pause on
set varabbrev off


/*----------------------
Defining workspace path
-----------------------*/

di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER."'  _request(path)
cd "$path"


/*----------------------
Setting folders to save outputs
-----------------------*/

shell mkdir "input"
shell mkdir "input/data"
shell mkdir "output"
shell mkdir "output/tables"

global input_data "input/data"
global output_tables "output/tables"



/*----------------------
Import ODBC dataset and
save temp
-----------------------*/
	
	
odbc load, exec("select * from jefas_gdp_revisions_base_year") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change the dataset loaded from SQL as preferred. 

cd "$path"
cd "$input_data"
	
save jefas_ts, replace



/*----------------------
Clean-up JEFAS dataset
-----------------------*/

use "jefas_ts.dta", clear


	* Clean-up date
	
	* Step 1. If it is a datetime (%tc), convert to daily date (%td)
	gen daily = dofc(vintages_date)
	format daily %td

	* Step 2. Convert daily to monthly (%tm)
	gen monthly = mofd(daily)
	format monthly %tm
	
	* Drop no longer needed vars
	drop vintages_date daily
	rename monthly vintages_date
	
	* Rename
	rename vintages_date target_period
	
	order target_period

	
save "jefas_ts_cleaned.dta", replace


	
/*----------------------
Clean-up nowcasting dataset
-----------------------*/

use "fitted_vals.dta", clear


	* Clean-up
	
	keep target_period y_hat_* y_12
	
	rename y_12 y_hat_12
	
	forvalues h = 1/12 {
		rename y_hat_`h' gdp_release_hat_`h'
	}
	 
	order target_period gdp_release_hat_*
	
save "fitted_vals_ts.dta", replace
	
	
/*----------------------
Merge both datasets
-----------------------*/
	
use "jefas_ts_cleaned.dta", clear

	merge 1:1 target_period using fitted_vals_ts

	drop _merge

	sort target_period gdp_release_* gdp_release_hat_*
	order target_period gdp_release_* gdp_release_hat_*
	
	rename gdp_release_hat_12 gdp_most_recent_hat

save "jefas_nowcasting_ts.dta", replace

export delimited using "jefas_nowcasting_ts.csv", replace

	