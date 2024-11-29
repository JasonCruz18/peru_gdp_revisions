/********************
Variance Bounds Tests
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: variance_bounds_tests.do
		** 	First Created: 09/15/24
		** 	Last Updated:  10/11/24
			
	***
	** Just click on the "Run (do)" button, the code will do the rest for you.
	***

	
	
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
			
	//log using efficiency_tests.txt, text replace // Opens a log file and replaces it if it exists.

	

	/*----------------------
	Defining workspace path
	------------------------*/
	
	di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER."'  _request(path)
	
	cd "$path"
		
		
		
	/*----------------------
	Setting folders to save outputs
	------------------------*/
		
	shell mkdir "output" 			// Creates folder to save outputs.
	//shell mkdir "output/graphs" 	// Creates folder to save graphs.
	shell mkdir "output/tables" 	// Creates folder to save tables.
	//shell mkdir "output/data" 	// Creates folder to save data.
		
	
	* Set as global vars
	
	//global graphs_folder "output/charts"	// Use to export charts.
	global tables_folder "output/tables"	// Use to export tables.
	//global data_folder "output/data"		// Use to export .dta.
	
		
		
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
		
		
	odbc load, exec("select * from sectorial_gdp_monthly_cum_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save temp_cum_panel, replace


	
	/*----------------------
	On-the-fly data cleaning
	-----------------------*/

	
	use temp_cum_panel.dta, clear

	
		* Sort by date and horizon
		
		sort vintages_date horizon // Key step to set both the ID and time vars for panel data.

		
		* At a glance (inspect data)

		d // Check entire dataset vars to understand its structure.
		sum // Summarize stats for all vars in the dataset (mean, standard deviation, etc.).
		count // Count the total number of observations, expected to be 6,696.

		
		* Fixing date format
		
		gen numeric_date = dofc(vintages_date) // To a Stata date in days.
		format numeric_date %td // To standard Stata date (e.g., day-month-year).

		gen target_date = mofd(numeric_date) // To a monthly date format.
		format target_date %tm // To standard Stata month (e.g., Jan 2023).

		drop vintages_date numeric_date // Drop the original vars since they are no longer needed.

		order target_date horizon // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.
		
		
		* Generate time-trend var
		
		** Get max value from horizon
		//egen max_horizon = max(horizon)
		
		** Gen new var as the difference between max_horizon and horizon
		//gen time_trend = max_horizon - horizon // This is a kind of trend var (H-h)
		
		
		* Define the panel data structure
		
		xtset target_date horizon

		
		* Set global sectors
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
		
		
		* Generate dependent variables for regressions
		
		foreach sector of global sectors {
			gen log_abs_e_`sector' = ln(abs(e_`sector'))
		}
		
		foreach sector of global sectors {
			gen log_sq_e_`sector' = ln((e_`sector')^2)
		}

		
	save temp_cum_panel_cleaned.dta, replace
	
	
		
	/*----------------------
	Regression for the abs value
	of the nowcast error (1/2)
	________________________
	Paper version
	-----------------------*/
	
	
	use temp_cum_panel_cleaned.dta, clear
	preserve
		
		
		* Clean up any previous estimates
		
		estimates clear	
		
		
		* Create a regression program to process each sector
		
		program define reg_abs_1
			args sector model_type
			
			local dep_var log_abs_e_`sector' // Dependent var
			local indep_vars horizon // Independent var
			
			* Execute model according to type (fe, re, xtscc_fe xtscc_re)
			
			if "`model_type'" == "fe" { // Fixed-effects regression (within): standard errors adjusted by 28 clusters
				xtreg `dep_var' `indep_vars', fe vce(cluster target_date)
			}
			else if "`model_type'" == "re" { // Fixed-effects regression: Driscoll-Kraay standard errors
				xtreg `dep_var' `indep_vars', re vce(cluster target_date)
			}
			else if "`model_type'" == "xtscc_fe" { // Random-effects regression (GLS): standard errors adjusted for 28 clusters
				xtscc `dep_var' `indep_vars', fe
			}
			else if "`model_type'" == "xtscc_re" { // Random-effects regression (GLS): Driscoll-Kraay standard errors
				xtscc `dep_var' `indep_vars', re
			}
			
			* Save the F-stat and p-value values
			
			//scalar F_value = r(F)
			//scalar p_value = r(p)
			//estadd scalar F_`sector' F_value
			//estadd scalar p_`sector' p_value
			
			* Obtain panel dimensions with xtsum
			
			xtsum `dep_var'
			scalar obs_between = r(n)     // Number of groups (between)
			scalar obs_within = r(Tbar)   // Average number of periods per group (within)
			scalar obs_total = r(N)       // Number of obs
			
			* Add the values of observations to the results
			
			estadd scalar n_`sector' obs_between
			estadd scalar h_`sector' obs_within
			estadd scalar N_`sector' obs_total
			
			* Save model results
			
			estimates store `model_type'_`sector'
		end

		
		* Loop to run regressions for each sector
		
		foreach sector of global sectors {
			
			* Assign full sector name
			
			if "`sector'" == "gdp" local sector_name "PBI"
			else if "`sector'" == "agriculture" local sector_name "Agropecuario"
			else if "`sector'" == "fishing" local sector_name "Pesca"
			else if "`sector'" == "mining" local sector_name "Minería e Hidrocarburos"
			else if "`sector'" == "manufacturing" local sector_name "Manufactura"
			else if "`sector'" == "electricity" local sector_name "Electricidad y Agua"
			else if "`sector'" == "construction" local sector_name "Construcción"
			else if "`sector'" == "commerce" local sector_name "Comercio"
			else if "`sector'" == "services" local sector_name "Otros Servicios"
			else local sector_name "`sector'"  // By default, use the original name if no mapping is found.
			
			* FE (cluster by events)
			
			reg_abs_1 `sector' fe
			
			* FE (Driscoll-Kraay)
			
			reg_abs_1 `sector' xtscc_fe
			
			* RE (cluster by events)
			
			reg_abs_1 `sector' re

			* RE (Driscoll-Kraay)
			
			reg_abs_1 `sector' xtscc_re
			
			* Report results using esttab
			
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "abs_error.tex", append ///
				b(%9.3f) se(%9.3f) stats(n_`sector' h_`sector' N_`sector', label("n" "$\bar{h}$" "N") fmt(%9.0f %9.0f %9.0f)) ///
				order(_cons) longtable ///
				varlabels(_cons "Intercepto" horizon "h") ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				booktabs style(tex) nodepvars nomtitle ///
				posthead("\hline\multicolumn{5}{c}{\textit{`sector_name'}} \\ \hline") ///
				nonotes
		}
	
	
	
	/*----------------------
	Regression for the abs value
	of the nowcast error (1/2)
	________________________
	Presentation version
	----------------------*/
		
		
		* Clean up any previous estimates
		
		estimates clear	

		* Create a regression program to process each sector
		
		program define reg_abs_2
			args sector model_type
			
			local dep_var log_abs_e_`sector' // Dependent var
			local indep_vars horizon // Independent var
			
			* Execute model according to type (fe, re, xtscc_fe xtscc_re)
			
			if "`model_type'" == "fe" { // Fixed-effects regression (within): standard errors adjusted by 28 clusters
				xtreg `dep_var' `indep_vars', fe vce(cluster target_date)
			}
			else if "`model_type'" == "re" { // Fixed-effects regression: Driscoll-Kraay standard errors
				xtreg `dep_var' `indep_vars', re vce(cluster target_date)
			}
			else if "`model_type'" == "xtscc_fe" { // Random-effects regression (GLS): standard errors adjusted for 28 clusters
				xtscc `dep_var' `indep_vars', fe
			}
			else if "`model_type'" == "xtscc_re" { // Random-effects regression (GLS): Driscoll-Kraay standard errors
				xtscc `dep_var' `indep_vars', re
			}
			
			* Save the F-stat and p-value values
			
			//scalar F_value = r(F)
			//scalar p_value = r(p)
			//estadd scalar F_`sector' F_value
			//estadd scalar p_`sector' p_value
			
			* Obtain panel dimensions with xtsum
			
			xtsum `dep_var'
			scalar obs_between = r(n)     // Number of groups (between)
			scalar obs_within = r(Tbar)   // Average number of periods per group (within)
			scalar obs_total = r(N)       // Number of obs
			
			* Add the values of observations to the results
			
			estadd scalar n_`sector' obs_between
			estadd scalar h_`sector' obs_within
			estadd scalar N_`sector' obs_total
			
			* Save model results
			
			estimates store `model_type'_`sector'
		end


		* Loop to run regressions for each sector
		
		foreach sector of global sectors {
	
			* Assign full sector name
			
			if "`sector'" == "gdp" local sector_name "PBI"
			else if "`sector'" == "agriculture" local sector_name "Agropecuario"
			else if "`sector'" == "fishing" local sector_name "Pesca"
			else if "`sector'" == "mining" local sector_name "Minería e Hidrocarburos"
			else if "`sector'" == "manufacturing" local sector_name "Manufactura"
			else if "`sector'" == "electricity" local sector_name "Electricidad y Agua"
			else if "`sector'" == "construction" local sector_name "Construcción"
			else if "`sector'" == "commerce" local sector_name "Comercio"
			else if "`sector'" == "services" local sector_name "Otros Servicios"
			else local sector_name "`sector'"  // By default, use the original name if no mapping is found.

			* FE (cluster by events)
			
			reg_abs_2 `sector' fe
			
			* FE (Driscoll-Kraay)
			
			reg_abs_2 `sector' xtscc_fe
			
			* RE (cluster by events)
			
			reg_abs_2 `sector' re

			* RE (Driscoll-Kraay)
			
			reg_abs_2 `sector' xtscc_re
			
			* Report results using esttab
			
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "abs_error_`sector'.tex", ///
			b(%9.3f) se(%9.3f) stats(n_`sector' h_`sector' N_`sector', label("n" "$\bar{h}$" "N") fmt(%9.0f %9.0f %9.0f)) ///
			order(_cons) ///
			varlabels(_cons "Intercepto" horizon "h") ///
			noobs ///
			star(* 0.1 ** 0.05 *** 0.01) ///
			booktabs style(tex) nodepvars nomtitle ///
			posthead("\hline\multicolumn{5}{c}{\textit{`sector_name'}} \\ \hline") ///
			nonotes
		}
			
			
	save temp_cum_panel_cleaned.dta, replace
	return
	
	
	
	/*----------------------
	Drop aux data and tables
	-----------------------*/	

	* List all .dta, .txt and .tex files in the current directory and store in a local macro
	
	local dta_files : dir . files "*.dta"
	local txt_files : dir . files "*.txt"
	local tex_files : dir . files "*.tex"

	* Iterate over each .dta file and delete it
	
	foreach file of local dta_files {
		erase "`file'"
	}	
	
	* Iterate over each .txt file and delete it
	
	foreach file of local txt_files {
		erase "`file'"
	}	
	
	* Iterate over each .tex file and delete it
	
	foreach file of local tex_files {
		erase "`file'"
	}
	
	