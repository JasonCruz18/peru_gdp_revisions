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
	
	
odbc load, exec("select * from jefas_gdp_revisions_base_year_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change the dataset loaded from SQL as preferred. 

cd "$path"
cd "$input_data"
	
save jefas_panel, replace



/*----------------------
Clean-up JEFAS dataset
-----------------------*/

use "jefas_panel.dta", clear


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
	
	destring horizon, replace

	
save "jefas_panel_cleaned.dta", replace


	
/*----------------------
Clean-up nowcasting dataset
-----------------------*/

use "fitted_vals.dta", clear


	* Clean-up
	
	keep target_period y_hat_* y_12
	
	rename y_12 y_hat_12
	
	order target_period y_hat_1 y_hat_2 y_hat_3 y_hat_4 y_hat_5 y_hat_6 y_hat_7 y_hat_8 y_hat_9 y_hat_10 y_hat_11 y_hat_12
	
	* To long data
	
	reshape long y_hat_, i(target_period) j(horizon)
	
	rename y_hat_ gdp_release_hat
	
	
save "fitted_vals_panel.dta", replace
	
	
/*----------------------
Merge both datasets
-----------------------*/
	
use "jefas_panel_cleaned.dta", clear

	merge 1:1 target_period horizon using fitted_vals_panel

	drop _merge

	sort target_period horizon gdp_release gdp_release_hat
	order target_period horizon gdp_release gdp_release_hat


save "jefas_nowcasting_panel.dta", replace

export delimited using "jefas_nowcasting_panel.csv", replace

	