/********************
Nowcasting GDP Revisions — EWS
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
Time Series Analysis
-----------------------*/

cd "$input_data"
use e_gdp_revisions_ts, clear


/*----------------------
Clean-up at a glance
-----------------------*/

drop bench_*


/*----------------------
 Define split (train / eval)
-----------------------*/
* breakpoint = end of 2013 -> train <= 2013m12 ; eval > 2013m12
gen double tm = target_period
gen byte train = (tm <= tm(2013m12))
gen byte eval  = (tm >  tm(2013m12))

* Quick check
tab train eval


/*----------------------
EWS construction
-----------------------*/

tsset target_period, monthly
local delta = 0.5

forvalues h = 1/12 {
	gen Y_ews_`h' = .
	quietly replace Y_ews_`h' = y_`h' in 1
	forvalues t = 2/`=_N' {
		quietly replace Y_ews_`h' = `delta'*L1.Y_ews_`h' + y_`h' in `t' if !missing(y_`h') & !missing(L1.Y_ews_`h')
		quietly replace Y_ews_`h' = L1.Y_ews_`h' in `t' if missing(y_`h')
	}
}

forvalues h = 2/12 {
	gen R_ews_`h' = .
	quietly replace R_ews_`h' = r_`h' in 1
	forvalues t = 2/`=_N' {
		quietly replace R_ews_`h' = `delta'*L1.R_ews_`h' + r_`h' in `t' if !missing(r_`h') & !missing(L1.R_ews_`h')
		quietly replace R_ews_`h' = L1.R_ews_`h' in `t' if missing(r_`h')
	}
}

forvalues h = 3/12 {
	gen L1_R_ews_`h' = L1.R_ews_`h'
}

tempfile ewma
save `ewma'


/*----------------------
Omnibus regressions
-----------------------*/

qui {
    tsset target_period, monthly
    newey e_1 y_1 L1.e_1, lag(6) force
    predict residuals_aux, resid
}
keep if !missing(residuals_aux)
drop residuals_aux

forvalues h = 1/11 {
    if `h' == 1 {
        newey e_`h' L1.e_`h' y_`h' if train==1, lag(6) force
        matrix b = e(b)
        gen alpha_`h' = b[1, "_cons"]
        gen theta_`h' = b[1, "y_`h'"]
        gen delta_`h' = b[1, "L1.e_`h'"]
    }
    else if `h' == 2 {
        newey e_`h' L1.e_`h' y_`h' r_`h' if train==1, lag(6) force
        matrix b = e(b)
        gen alpha_`h' = b[1, "_cons"]
        gen theta_`h' = b[1, "y_`h'"]
        gen delta_`h' = b[1, "L1.e_`h'"]
        gen gamma_`h' = b[1, "r_`h'"]
    }
    else {
        newey e_`h' L1.e_`h' y_`h' r_`h' L1.r_`h' if train==1, lag(6) force
        matrix b = e(b)
        gen alpha_`h' = b[1, "_cons"]
        gen theta_`h' = b[1, "y_`h'"]
        gen delta_`h' = b[1, "L1.e_`h'"]
        gen gamma_`h' = b[1, "r_`h'"]
        gen rho_`h'   = b[1, "L1.r_`h'"]
    }
}

tempfile coeffs
save `coeffs', replace


/*----------------------
Fitted values (raw correction)
-----------------------*/

forvalues h = 1/11 {
    gen e_hat_`h' = .
    if `h' == 1 {
        replace e_hat_`h' = (alpha_`h')/(1 - `delta') + theta_`h'*Y_ews_`h'
    }
    else if `h' == 2 {
        replace e_hat_`h' = (alpha_`h')/(1 - `delta') + theta_`h'*Y_ews_`h' + gamma_`h'*R_ews_`h'
    }
    else {
        replace e_hat_`h' = (alpha_`h')/(1 - `delta') + theta_`h'*Y_ews_`h' + gamma_`h'*R_ews_`h' + rho_`h'*L1_R_ews_`h'
    }
}

forvalues h = 1/11 {
    gen y_hat_`h' = y_`h' + e_hat_`h'
}

forvalues h = 1/11 {
    gen e_now_`h' = .
	replace e_now_`h' = y_12 - y_hat_`h'
}


/*----------------------
Bias-scaling adjustment
-----------------------*/

/*
forvalues h = 1/11 {
    quietly summarize e_`h' if train==1
    scalar mean_train = r(mean)
    quietly summarize e_`h' if eval==1
    scalar mean_eval = r(mean)

    * scaling factor λ_h = mean_eval / mean_train
    scalar lambda = cond(mean_train!=0, mean_eval/mean_train, 1)

    * apply scaling only on eval subsample
    replace e_now_`h' = lambda * e_now_`h' if eval==1
}
*/

save "fitted_vals.dta", replace


/*----------------------
Forecast evaluation
-----------------------*/

use "fitted_vals.dta", clear

* Relative MAE, RMSE, MAPE vs benchmark
tempfile rmse_results
postfile pf_rmse h rmse using `rmse_results', replace

forvalues h = 1/11 {

	gen sq_now = (e_now_`h')^2 if eval==1
	gen sq_bench = (e_`h')^2 if eval==1
	quietly summarize sq_now
	local rmse_now = sqrt(r(mean))
	quietly summarize sq_bench
	local rmse_bench = sqrt(r(mean))
	drop sq_now sq_bench
	local rmse_rel = `rmse_now' / `rmse_bench'

	post pf_rmse (`h') (`rmse_rel')
}
postclose pf_rmse
use `rmse_results', clear

gen rmse100 = rmse*100
save "rmse_results.dta", replace
export excel h rmse100 using "nowcasting_rel_perf.xlsx", firstrow(variables) replace


/*----------------------
DM test
-----------------------*/

use "fitted_vals.dta", clear
forvalues h = 1/11 {
	gen d_`h'_dm = (e_now_`h')^2 - (e_`h')^2
}
postfile pf_dm h dm_stat using "dm_results.dta", replace
forvalues h = 1/11 {
	newey d_`h'_dm if eval==1, lag(6) force
	scalar dm_stat_`h' = _b[_cons] / _se[_cons]
	post pf_dm (`h') (dm_stat_`h')
}
postclose pf_dm


/*----------------------
Encompassing test
-----------------------*/

use "fitted_vals.dta", clear
postfile pf_encom h beta tstat using "encom_results.dta", replace
forvalues h = 1/11 {
	gen d_`h'_encom = e_`h' - e_now_`h'
	newey e_`h' d_`h'_encom if eval==1, lag(6) force
	scalar beta_`h' = _b[d_`h'_encom]
	scalar tstat_`h' = _b[d_`h'_encom]/_se[d_`h'_encom]
	post pf_encom (`h') (beta_`h') (tstat_`h')
}
postclose pf_encom


/*----------------------
Merge and Final Export
-----------------------*/

use "rmse_results.dta", clear
merge 1:1 h using "dm_results.dta"
drop _merge
merge 1:1 h using "encom_results.dta"
drop _merge

keep h rmse100 dm_stat tstat
order h rmse100 dm_stat tstat

label var h       "Horizon"
label var rmse100 "RMSE (Bench=100)"
label var dm_stat "DM stat"
label var tstat   "Encompassing t-stat β"

export excel using "Nowcasting_Performance_EVAL_split_2013.xlsx", firstrow(varlabels) replace


	/*----------------------
	Plots
	-----------------------*/

use "fitted_vals.dta", clear
	tsset target_period, monthly

	twoway (tsline e_1 e_now_1 if tin(2013m2,2022m12), cmissing(n))
	
