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
Time Series Analysis
-----------------------*/

cd "$input_data"
use e_gdp_revisions_ts, clear


/*----------------------
EWMA construction
-----------------------*/

tsset target_period, monthly
local delta = 0.5

forvalues h = 1/11 {
    gen Yewma_`h' = .
    quietly replace Yewma_`h' = y_`h' in 1
    forvalues t = 2/`=_N' {
        quietly replace Yewma_`h' = `delta'*y_`h' + (1-`delta')*L.Yewma_`h' in `t'
    }

    if `h' > 1 {
        gen Rewma_`h' = .
        quietly replace Rewma_`h' = r_`h' in 1
        forvalues t = 2/`=_N' {
            quietly replace Rewma_`h' = `delta'*r_`h' + (1-`delta')*L.Rewma_`h' in `t'
        }
    }
}

* Save the dataset with EWMA for later merge
tempfile ewma
save `ewma'


/*----------------------
Omnibus regressions
-----------------------*/

tempfile results
postfile handle str8 horizon double b_y b_r b_Lr b_Le Nobs using `results'

forvalues h = 1/11 {
    if `h' == 1 {
        newey e_`h' y_`h' L1.e_`h', lag(6) force
        matrix b = e(b)
        post handle ("h`h'") (b[1,1]) (.) (.) (b[1,2]) (e(N))
    }
    else if `h' == 2 {
        newey e_`h' r_`h' y_`h' L1.e_`h', lag(6) force
        matrix b = e(b)
        post handle ("h`h'") (b[1,2]) (b[1,1]) (.) (b[1,3]) (e(N))
    }
    else {
        newey e_`h' y_`h' r_`h' L1.r_`h' L1.e_`h', lag(6) force
        matrix b = e(b)
        post handle ("h`h'") (b[1,1]) (b[1,2]) (b[1,3]) (b[1,4]) (e(N))
    }
}
postclose handle

use `results', clear
gen h = real(substr(horizon,2,.))
sort h

tempfile coeffs
save `coeffs', replace


/*----------------------
Fitted values
-----------------------*/

use `ewma', clear
merge 1:1 _n using `coeffs', nogen keep(master match)

* Re-establish the time series structure after merge
tsset target_period, monthly

forvalues h = 1/11 {
    gen yhat_`h' = .
    if `h' == 1 {
        replace yhat_`h' = b_y*Yewma_`h' + b_Le*L.e_`h'
    }
    else if `h' == 2 {
        replace yhat_`h' = b_y*Yewma_`h' + b_r*Rewma_`h' + b_Le*L.e_`h'
    }
    else {
        replace yhat_`h' = b_y*Yewma_`h' + b_r*Rewma_`h' + b_Lr*L.Rewma_`h' + b_Le*L.e_`h'
    }
}


save "fitted_vals.dta", replace


/*----------------------
Forecast evaluation
-----------------------*/

tempfile evals
postfile handle str8 horizon double ME RMSE MAE using `evals'

forvalues h = 1/11 {
    gen err_`h' = e_`h' - yhat_`h'
    quietly summarize err_`h'
    local ME = r(mean)
    quietly summarize err_`h', detail
    local MAE = r(sum_w)/r(N)
    local RMSE = sqrt(r(sum_w)/r(N))
    post handle ("h`h'") (`ME') (`RMSE') (`MAE')
}
postclose handle

use `evals', clear
save "eval_metrics.dta", replace


/*----------------------
Plots
-----------------------*/

use "fitted_vals.dta", clear
tsset target_period, monthly

twoway (tsline yhat_1 y_1 y_12 if tin(2015m1,2022m12)), ///
    title("Nowcast vs. true GDP growth — Horizon 1")

twoway (tsline yhat_2 y_2 y_12 if tin(2015m1,2022m12)), ///
    title("Nowcast vs. true GDP growth — Horizon 2")

twoway (tsline yhat_3 y_3 y_12 if tin(2015m1,2022m12)), ///
    title("Nowcast vs. true GDP growth — Horizon 3")




