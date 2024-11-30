/********************
Efficiency Cross-Sectors
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: a_efficiency_test_extended.do
		** 	First Created: 10/11/24
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
	//shell mkdir "output/charts" 	// Creates folder to save charts.
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
		
		
	odbc load, exec("select * from sectorial_gdp_annual_int_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save int_temp_panel, replace

	
	
	/*----------------------
	On-the-fly data cleaning
	-----------------------*/

	
	use int_temp_panel, clear


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

		
		* Define the panel data structure
		
		xtset target_date horizon
	
	
	save int_temp_panel_cleaned, replace
	

	
	/*----------------------
	Extended cross-sector
	efficiency regression
	________________________
	Paper and presentation
	version
	-----------------------*/


	use int_temp_panel_cleaned, clear
	preserve // Save the current dataset state

		* Set global sectors
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services

		* Clean up any previous estimates
		
		estimates clear

		* Create a regression program to process each model
		
		program define reg_extended_efficiency
			args model_type
			
			local dep_var r_gdp  // Dependent variable
			local indep_vars     // Independent variables

			* Execute model according to type (fe, re, xtscc_fe xtscc_re)
			
			local indep_vars L2.r_gdp
			
			foreach sector of global sectors { 
				local indep_vars `indep_vars' L1.r_`sector'
			}

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

			* Obtain panel dimensions with xtsum
			
			xtsum `dep_var'
			scalar n_model = r(n)       // Number of groups (between)
			scalar h_model = r(Tbar)    // Average number of periods per group (within)
			scalar N_model = r(N)       // Number of obs

			* Add the values of observations to the results
			
			estadd scalar n_model = n_model
			estadd scalar h_model = h_model
			estadd scalar N_model = N_model

			* Test restrictions
			
			local restriction
			
			foreach sector of global sectors {
				local restriction `restriction' (L1.r_`sector' = 0)
			}
			
			local restriction `restriction' (L2.r_gdp = 0)
			test `restriction'
			
			test `restriction'

			* Add F-statistic or Chi2 based on model type
			
			if "`model_type'" == "re" {
				estadd scalar chi2_stat = r(chi2) // Report Chi2 for model (3)
			}
			else {
				estadd scalar chi2_stat = 2*r(F) // Report F-stat for all other models
			}
			
			* Add p-value
			
			estadd scalar p_val = r(p)

			* Store the model
			
			estimates store `model_type'
		end

		* Run regressions for all models
		
		local all_models
		
		foreach model_type in fe xtscc_fe re xtscc_re {
			reg_extended_efficiency `model_type'
			local all_models `all_models' `model_type'
		}

		* Generate combined table with results
		
		esttab `all_models' using "a_cross-sectors.tex", replace ///
			b(%9.3f) se(%9.3f) stats(chi2_stat p_val n_model h_model N_model, ///
			label("Chi2" "p-value" "n" "$\bar{h}$" "N") ///
				fmt(%9.3f %9.3f %9.0f %9.0f %9.0f)) ///
			order(_cons L.r_gdp L2.r_gdp) ///
			varlabels(_cons "Intercepto" L.r_gdp "r(-1)" L2.r_gdp "r(-2)" L.r_agriculture "r(-1): Agropecuario" L.r_fishing "r(-1): Pesca" L.r_manufacturing "r(-1): Manufactura" L.r_mining "r(-1): Minería e Hidrocarburos" L.r_construction "r(-1): Construcción" L.r_commerce "r(-1): Comercio" L.r_services "r(-1): Otros Servicios" L.r_electricity "r(-1): Electricidad y Agua") ///
			noobs ///
			star(* 0.1 ** 0.05 *** 0.01) ///
			booktabs style(tex) nodepvars nomtitle ///
			posthead("\hline\multicolumn{5}{c}{\textit{PBI}} \\ \hline") ///
			nonotes

	* Restore the original dataset
	
	restore
		
	
	
	/*----------------------
	Drop aux data and tables
	-----------------------*/	

	// List all .dta, .txt and .tex files in the current directory and store in a local macro
	
	local dta_files : dir . files "*.dta"
	//local txt_files : dir . files "*.txt"
	//local tex_files : dir . files "*.tex"

	// Iterate over each .dta file and delete it
	
	foreach file of local dta_files {
		erase "`file'"
	}	
	
	// Iterate over each .txt file and delete it
	
	//foreach file of local txt_files {
	//	erase "`file'"
	//}	
	
	// Iterate over each .tex file and delete it
	
	//foreach file of local tex_files {
	//	erase "`file'"
	//}	
	
		