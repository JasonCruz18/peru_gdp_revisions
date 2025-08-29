/********************
Nowcasting GDP Revisions — EWS + Forecast Combination
***

	Author
	---------------------
	D & J
	*********************/

	*** Program: nowcasting.do
	** 	First Created: 08/11/25
	** 	Last Updated:  08/29/25
		
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
    gen y_hat_`h' = y_`h' + e_hat_`h'
}

save "fitted_vals.dta", replace


/*----------------------
Forecast combination with releases (real-time expanding OLS)
-----------------------*/

* Settings
local minobs = 24    // minimum past observations to estimate lambda
local bound_lambda = 1 // bound lambda in [0,1]

use "fitted_vals.dta", clear

* Prepare storage for combined forecasts and errors
foreach h of numlist 1/11 {
    gen double y_comb_`h' = .
    gen double e_comb_`h' = .
    gen double lambda_comb_`h' = .
}

* list of evaluation dates
levelsof target_period if eval==1, local(eval_dates)

* Loop horizons
foreach h of numlist 1/11 {
    di as txt "== Combination for horizon h=`h' =="

    foreach t of local eval_dates {

        * Past-sample for expanding-window OLS
        quietly {
            gen double dep_past = e_`h' if target_period < `t' & !missing(e_`h')
            gen double ind_past = e_hat_`h' if target_period < `t' & !missing(e_hat_`h')
        }

        quietly count if !missing(dep_past, ind_past)
        local Npast = r(N)

        if `Npast' >= `minobs' {
            quietly regress dep_past ind_past if !missing(dep_past, ind_past), noconstant
            local lambda = _b[ind_past]
            if `bound_lambda'==1 {
                if `lambda'<0 local lambda = 0
                if `lambda'>1 local lambda = 1
            }
        }
        else local lambda = 0.5

        * Apply combination to evaluation date `t'
        quietly replace y_comb_`h' = `lambda'*y_hat_`h' + (1-`lambda')*y_`h' if target_period==`t'
        quietly replace e_comb_`h' = y_12 - y_comb_`h' if target_period==`t'
        quietly replace lambda_comb_`h' = `lambda' if target_period==`t'

        quietly drop dep_past ind_past
    }

    di as txt "Finished combination for h=`h'"
}

save "nowcast_combined_series.dta", replace


/*----------------------
Forecast evaluation (RMSE + DM + Encompassing)
-----------------------*/


* RMSE relative to benchmark
tempfile rmse_results
postfile pf_rmse h rmse using `rmse_results', replace

forvalues h = 1/11 {
	gen double sq_comb = (e_comb_`h')^2 if eval==1
	gen double sq_bench = (e_`h')^2 if eval==1
	quietly summarize sq_comb if !missing(sq_comb)
	local rmse_comb = sqrt(r(mean))
	quietly summarize sq_bench if !missing(sq_bench)
	local rmse_bench = sqrt(r(mean))
	local rmse_rel = `rmse_comb'/`rmse_bench'
	post pf_rmse (`h') (`rmse_rel')
	drop sq_comb sq_bench
}
postclose pf_rmse
use `rmse_results', clear
gen rmse100 = rmse*100
save "rmse_results.dta", replace

use "nowcast_combined_series.dta", clear

* Diebold-Mariano test comparing combined vs benchmark
tempfile dm_comb
postfile pf_dm h dm_stat using `dm_comb', replace

forvalues h = 1/11 {
	gen double d_comb_`h' = . 
	replace d_comb_`h' = (e_comb_`h')^2 - (e_`h')^2 if eval==1 & !missing(e_comb_`h', e_`h')
	quietly newey d_comb_`h' if eval==1, lag(6) force
	local b_cons = _b[_cons]
	local se_cons = _se[_cons]
	local dm = cond("`b_cons'"!="" & "`se_cons'"!="", `b_cons'/`se_cons', .)
	post pf_dm (`h') (`dm')
	drop d_comb_`h'
}
postclose pf_dm
use `dm_comb', clear

use "nowcast_combined_series.dta", clear

* Encompassing test
postfile pf_encom h beta tstat using "encom_results.dta", replace
forvalues h = 1/11 {
	gen d_`h'_encom = e_`h' - y_comb_`h'
	newey e_`h' d_`h'_encom if eval==1, lag(6) force
	scalar beta_`h' = _b[d_`h'_encom]
	scalar tstat_`h' = _b[d_`h'_encom]/_se[d_`h'_encom]
	post pf_encom (`h') (beta_`h') (tstat_`h')
}
postclose pf_encom

* Merge and export
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

export excel using "Nowcasting_Performance_EVAL_split_2013_1.xlsx", firstrow(varlabels) replace


	/*----------------------
	Plots
	-----------------------*/

use "nowcast_combined_series.dta", clear
	tsset target_period, monthly

	twoway (tsline e_1 if !missing(e_1), cmissing(n)) ///
       (tsline e_hat_1 if !missing(e_hat_1), cmissing(n)) ///
       (tsline e_comb_1 if !missing(e_comb_1), cmissing(n)) ///
       if tin(2013m2,2022m12)
	
	
	
