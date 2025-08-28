/********************************************************************
 Nowcasting GDP Revisions — EWMA (Split-sample evaluation)
 Author: D & J
 Created: 08/11/25
 Updated: <today>
********************************************************************/

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
 Workspace path (INPUT)
-----------------------*/
di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER."'  _request(path)
cd "$path"

/*----------------------
 Folders
-----------------------*/
shell mkdir "input"
shell mkdir "input/data"
shell mkdir "output"
shell mkdir "output/tables"

global input_data "input/data"
global output_tables "output/tables"

/*----------------------
 Load data (time-series panel)
-----------------------*/
cd "$input_data"
use e_gdp_revisions_ts, clear

/*----------------------
 Drop bench vars not used here
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
 EWS construction (same as before)
-----------------------*/
tsset target_period, monthly
local delta = 0.5

forvalues h = 1/12 {
    quietly {
        gen double Y_ews_`h' = .
        replace Y_ews_`h' = y_`h' in 1
        forvalues t = 2/`=_N' {
            replace Y_ews_`h' = `delta'*L1.Y_ews_`h' + y_`h' in `t' if !missing(y_`h') & !missing(L1.Y_ews_`h')
            replace Y_ews_`h' = L1.Y_ews_`h' in `t' if missing(y_`h')
        }
    }
}

forvalues h = 2/12 {
    quietly {
        gen double R_ews_`h' = .
        replace R_ews_`h' = r_`h' in 1
        forvalues t = 2/`=_N' {
            replace R_ews_`h' = `delta'*L1.R_ews_`h' + r_`h' in `t' if !missing(r_`h') & !missing(L1.R_ews_`h')
            replace R_ews_`h' = L1.R_ews_`h' in `t' if missing(r_`h')
        }
    }
}

forvalues h = 3/12 {
    gen double L1_R_ews_`h' = L1.R_ews_`h'
}

/* Save EWS dataset for reproducibility */
tempfile ewma
save `ewma', replace


/*----------------------
 Omnibus regressions (ESTIMATE ON TRAIN ONLY)
-----------------------*/
* We estimate coefficients using only train==1
tsset target_period, monthly

qui {
    tsset target_period, monthly
    newey e_1 y_1 L1.e_1, lag(6) force
    predict residuals_aux, resid
}
keep if !missing(residuals_aux)
drop residuals_aux

forvalues h = 1/11 {
    di as txt "Estimating omnibus for h=`h' on training sample..."
    if `h' == 1 {
        quietly newey e_`h' L1.e_`h' y_`h' if train==1, lag(6) force
        local a = _b[_cons]
        local theta = _b[y_`h']
        local delt = _b[L1.e_`h']
        * create constant vars with estimated values
        capture drop alpha_`h' theta_`h' delta_`h'
        gen double alpha_`h' = `a'
        gen double theta_`h' = `theta'
        gen double delta_`h' = `delt'
    }
    else if `h' == 2 {
        quietly newey e_`h' L1.e_`h' y_`h' r_`h' if train==1, lag(6) force
        local a = _b[_cons]
        local theta = _b[y_`h']
        local delt = _b[L1.e_`h']
        local gamma = _b[r_`h']
        capture drop alpha_`h' theta_`h' delta_`h' gamma_`h'
        gen double alpha_`h' = `a'
        gen double theta_`h' = `theta'
        gen double delta_`h' = `delt'
        gen double gamma_`h' = `gamma'
    }
    else {
        quietly newey e_`h' L1.e_`h' y_`h' r_`h' L1.r_`h' if train==1, lag(6) force
        local a = _b[_cons]
        local theta = _b[y_`h']
        local delt = _b[L1.e_`h']
        local gamma = _b[r_`h']
        local rho   = _b[L1.r_`h']
        capture drop alpha_`h' theta_`h' delta_`h' gamma_`h' rho_`h'
        gen double alpha_`h' = `a'
        gen double theta_`h' = `theta'
        gen double delta_`h' = `delt'
        gen double gamma_`h' = `gamma'
        gen double rho_`h'   = `rho'
    }
}

/* Save coeffs for record */
tempfile coeffs
save `coeffs', replace

/*----------------------
 Fitted values (apply TRAIN estimates to full sample; we'll evaluate on eval sample)
-----------------------*/
use `coeffs', clear
/* The dataset currently contains the coefficient variables (alpha_*, etc.)
   and also the original series because we saved from the same dataset.
   If you instead saved coeffs only, ensure you merge EWS back. */
* If missing Y_ews etc (shouldn't), reload ewma and merge coeffs -- but current flow keeps them.
	
forvalues h = 1/11 {
    capture confirm variable e_hat_`h'
    if _rc == 0 {
        drop e_hat_`h'
    }
    gen double e_hat_`h' = .
    quietly {
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
}

forvalues h = 1/11 {
    capture drop y_hat_`h'
    gen double y_hat_`h' = y_`h' + e_hat_`h'
}

/* Save fitted values */
save "fitted_vals.dta", replace

/*----------------------
 Forecast evaluation (ONLY on EVALUATION sample)
-----------------------*/
use "fitted_vals.dta", clear

* Create helper flag if needed
gen byte is_eval = (target_period > tm(2013m12))

save "fitted_vals.dta", replace

use "fitted_vals.dta", clear

/*----------------------
 Relative MAE, RMSE, MAPE vs benchmark (evaluation sample only)
-----------------------*/
tempfile rmse_results
postfile pf_rmse h mae rmse mape using `rmse_results', replace

forvalues h = 1/11 {
    * build evaluation-only helpers
    gen double abs_now = . 
    gen double abs_bench = .
    replace abs_now   = abs(e_hat_`h') if is_eval==1 & !missing(e_hat_`h')
    replace abs_bench = abs(e_`h')    if is_eval==1 & !missing(e_`h')

    quietly summarize abs_now if !missing(abs_now)
    local mae_now = r(mean)
    quietly summarize abs_bench if !missing(abs_bench)
    local mae_bench = r(mean)
    local mae_rel = .
    if `=r(N)' > 0 & `=r(N)' != . {
        local mae_rel = `mae_now' / `mae_bench'
    }
    
    drop abs_now abs_bench

    gen double sq_now = .
    gen double sq_bench = .
    replace sq_now   = (e_hat_`h')^2 if is_eval==1 & !missing(e_hat_`h')
    replace sq_bench = (e_`h')^2    if is_eval==1 & !missing(e_`h')

    quietly summarize sq_now if !missing(sq_now)
    local rmse_now = sqrt(r(mean))
    quietly summarize sq_bench if !missing(sq_bench)
    local rmse_bench = sqrt(r(mean))
    local rmse_rel = `rmse_now' / `rmse_bench'
    drop sq_now sq_bench

    gen double ape_now = .
    gen double ape_bench = .
    replace ape_now   = abs(e_hat_`h'/y_12) if is_eval==1 & !missing(e_hat_`h') & !missing(y_12) & (y_12!=0)
    replace ape_bench = abs(e_`h'/y_12)    if is_eval==1 & !missing(e_`h') & !missing(y_12) & (y_12!=0)

    quietly summarize ape_now if !missing(ape_now)
    local mape_now = 100*r(mean)
    quietly summarize ape_bench if !missing(ape_bench)
    local mape_bench = 100*r(mean)
    local mape_rel = `mape_now' / `mape_bench'
    drop ape_now ape_bench

    * If denom or numerator missing, mape_rel may be ., keep as is.
    post pf_rmse (`h') (`mae_rel') (`rmse_rel') (`mape_rel')
}
postclose pf_rmse

use `rmse_results', clear
gen mae100  = mae*100
gen rmse100 = rmse*100
gen mape100 = mape*100

save "rmse_results.dta", replace
export excel h mae100 rmse100 mape100 using "nowcasting_rel_perf_eval.xlsx", firstrow(variables) replace

/*----------------------
 Diebold–Mariano test (evaluation sample only)
-----------------------*/
use "fitted_vals.dta", clear

forvalues h = 1/11 {
    gen double d_`h'_dm = . 
    replace d_`h'_dm = (e_hat_`h')^2 - (e_`h')^2 if is_eval==1 & !missing(e_hat_`h') & !missing(e_`h')
}

postfile pf_dm h dm_stat using "dm_results_eval.dta", replace
forvalues h = 1/11 {
    quietly newey d_`h'_dm if is_eval==1, lag(6) force
    local b_cons = _b[_cons]
    local se_cons = _se[_cons]
    local dmstat = .
    if "`b_cons'" != "" & "`se_cons'" != "" {
        local dmstat = `b_cons' / `se_cons'
    }
    di as txt "DM stat (eval) h=`h' : " %6.4f `dmstat'
    post pf_dm (`h') (`dmstat')
}
postclose pf_dm

/*----------------------
 Encompassing test (evaluation sample only)
-----------------------*/
use "fitted_vals.dta", clear

postfile pf_encom h beta tstat using "encom_results_eval.dta", replace
forvalues h = 1/11 {
    gen double d_`h'_encom = .
    replace d_`h'_encom = e_`h' - e_hat_`h' if is_eval==1 & !missing(e_`h') & !missing(e_hat_`h')

    quietly newey e_`h' d_`h'_encom if is_eval==1, lag(6) force
    local beta = _b[d_`h'_encom]
    local se_beta = _se[d_`h'_encom]
    local tstat = .
    if "`beta'" != "" & "`se_beta'" != "" {
        local tstat = `beta' / `se_beta'
    }
    di as txt "Encomp (eval) h=`h' : beta=" %6.4f `beta' "  t=" %6.4f `tstat'
    post pf_encom (`h') (`beta') (`tstat')
}
postclose pf_encom

/*----------------------
 Merge final table (evaluation results) and export (compact columns)
-----------------------*/
use "rmse_results.dta", clear
merge 1:1 h using "dm_results_eval.dta"
drop _merge
merge 1:1 h using "encom_results_eval.dta"
drop _merge

* Keep only requested columns
keep h mae100 rmse100 mape100 dm_stat tstat
order h mae100 rmse100 mape100 dm_stat tstat

label var h       "Horizon"
label var mae100  "MAE (Bench=100)"
label var rmse100 "RMSE (Bench=100)"
label var mape100 "MAPE (Bench=100)"
label var dm_stat "DM stat"
label var tstat   "Encompassing t-stat β"

export excel using "Nowcasting_Performance_EVAL_split_2013.xlsx", firstrow(varlabels) replace

di as txt "Split-sample evaluation complete. Results saved in: `output_tables'"

