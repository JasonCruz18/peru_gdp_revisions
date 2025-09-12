/********************
Robustness Analysis
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: robustness_analysis_1.do
		** 	First Created: 11/05/24
		** 	Last Updated:  11/--/24
			
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
		
		
	odbc load, exec("select * from r_sectorial_gdp_monthly_affected_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save r_affected_panel, replace
	
	
	odbc load, exec("select * from r_sectorial_gdp_monthly_base_year_dummies_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save r_base_year_dummies_panel, replace
	
	
	odbc load, exec("select * from e_sectorial_gdp_monthly_affected_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save e_affected_panel, replace
	
	
	odbc load, exec("select * from e_sectorial_gdp_monthly_base_year_dummies_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save e_base_year_dummies_panel, replace
	
	
	odbc load, exec("select * from z_sectorial_gdp_monthly_affected_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save z_affected_panel, replace
	
	
	odbc load, exec("select * from z_sectorial_gdp_monthly_base_year_dummies_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save z_base_year_dummies_panel, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(r)
	-----------------------*/

	
	use r_affected_panel, clear

	
		* Sort by vintages_date and horizon
		
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


		* Sort by target_date and horizon
		
		sort target_date horizon
	
	
	save r_affected_panel_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(r-dummies)
	-----------------------*/

	
	use r_base_year_dummies_panel, clear

	
		* Sort by vintages_date and horizon
		
		sort vintages_date horizon // Key step to set both the ID and time vars for panel data.

		
		* Fixing date format
		
		gen numeric_date = dofc(vintages_date) // To a Stata date in days.
		format numeric_date %td // To standard Stata date (e.g., day-month-year).

		gen target_date = mofd(numeric_date) // To a monthly date format.
		format target_date %tm // To standard Stata month (e.g., Jan 2023).

		drop vintages_date numeric_date // Drop the original vars since they are no longer needed.

		order target_date horizon // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.

		
		* Sort by target_date and horizon
		
		sort target_date horizon
	
	save r_base_year_dummies_panel_cleaned, replace
	
	

	/*----------------------
	On-the-fly data cleaning
	(e)
	-----------------------*/

	
	use e_affected_panel, clear

	
		* Sort by vintages_date and horizon
		
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


		* Sort by target_date and horizon
		
		sort target_date horizon
	
	
	save e_affected_panel_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(e-dummies)
	-----------------------*/

	
	use e_base_year_dummies_panel, clear

	
		* Sort by vintages_date and horizon
		
		sort vintages_date horizon // Key step to set both the ID and time vars for panel data.

		
		* Fixing date format
		
		gen numeric_date = dofc(vintages_date) // To a Stata date in days.
		format numeric_date %td // To standard Stata date (e.g., day-month-year).

		gen target_date = mofd(numeric_date) // To a monthly date format.
		format target_date %tm // To standard Stata month (e.g., Jan 2023).

		drop vintages_date numeric_date // Drop the original vars since they are no longer needed.

		order target_date horizon // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.

		
		* Sort by target_date and horizon
		
		sort target_date horizon
	
	save e_base_year_dummies_panel_cleaned, replace
	

	
	/*----------------------
	On-the-fly data cleaning
	(z)
	-----------------------*/

	
	use z_affected_panel, clear

	
		* Sort by vintages_date and horizon
		
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


		* Sort by target_date and horizon
		
		sort target_date horizon
	
	
	save z_affected_panel_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(z-dummies)
	-----------------------*/

	
	use z_base_year_dummies_panel, clear

	
		* Sort by vintages_date and horizon
		
		sort vintages_date horizon // Key step to set both the ID and time vars for panel data.

		
		* Fixing date format
		
		gen numeric_date = dofc(vintages_date) // To a Stata date in days.
		format numeric_date %td // To standard Stata date (e.g., day-month-year).

		gen target_date = mofd(numeric_date) // To a monthly date format.
		format target_date %tm // To standard Stata month (e.g., Jan 2023).

		drop vintages_date numeric_date // Drop the original vars since they are no longer needed.

		order target_date horizon // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.

		
		* Sort by target_date and horizon
		
		sort target_date horizon
	
	save z_base_year_dummies_panel_cleaned, replace
	
	
	
	/*----------------------
	First moment regression
	(r)
	________________________
	Paper and presentation
	version
	-----------------------*/
	
	* r_t(h) = c + \beta Dummy	
	*.........................................................................
	
	
	use r_affected_panel_cleaned, clear

	
		* Make the merge with the second dataset
		
		merge 1:1 target_date horizon using r_base_year_dummies_panel_cleaned

		
		* Check the result of the merge
		
		tab _merge

		
		* Remove remarks that do not appear in both datasets (optional)
		
		keep if _merge == 3

		
		* Delete the _merge variable (optional)
		
		drop _merge
			
			
		* Define the panel data structure
		
		xtset target_date horizon

		
		* Create macro for sectors
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
		
	
		* Clear any previous estimates
		
		estimates clear	
		
		
		* Program to run each regression for each sector
		
		
		program define r_base_year_dummy_sector
			args sector model_type
			
			local dep_var r_`sector' // Dependent var
			local indep_vars i.r_dummy_`sector' // Independent var

			
			* Run the model according to type (fe, re, xtscc_fe, xtscc_re)
			
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
			
			r_base_year_dummy_sector `sector' fe
						
			* FE (Driscoll-Kraay)
			
			r_base_year_dummy_sector `sector' xtscc_fe

			* RE (cluster by events)
			
			r_base_year_dummy_sector `sector' re
			
			* RE (Driscoll-Kraay)
			
			r_base_year_dummy_sector `sector' xtscc_re
			
			* Report results using esttab
			
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "$tables_folder/r_robustness_analysis_m.tex", append ///
				b(%9.3f) se(%9.3f) stats(n_`sector' h_`sector' N_`sector', label("n" "$\bar{h}$" "N") fmt(%9.0f %9.0f %9.0f)) ///
				keep(_cons 1.r_dummy_`sector') ///
				varlabels(_cons "Intercepto" 1.r_dummy_`sector' "r-dummy") ///
				order(_cons) longtable ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				booktabs style(tex) nodepvars nomtitle ///
				posthead("\hline\multicolumn{5}{c}{\textit{`sector_name'}} \\ \hline") ///
				nonotes
		}
	
	
	
	/*----------------------
	First moment regression
	(e)
	________________________
	Paper and presentation
	version
	-----------------------*/
	
	* e_t(h) = c + \beta Dummy	
	*.........................................................................
	
	
	use e_affected_panel_cleaned, clear

	
		* Make the merge with the second dataset
		
		merge 1:1 target_date horizon using e_base_year_dummies_panel_cleaned

		
		* Check the result of the merge
		
		tab _merge

		
		* Remove remarks that do not appear in both datasets (optional)
		
		keep if _merge == 3

		
		* Delete the _merge variable (optional)
		
		drop _merge
			
			
		* Define the panel data structure
		
		xtset target_date horizon

		
		* Create macro for sectors
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
		
	
		* Clear any previous estimates
		
		estimates clear	
		
		
		* Program to run each regression for each sector
		
		
		program define e_base_year_dummy_sector
			args sector model_type
			
			local dep_var e_`sector' // Dependent var
			local indep_vars i.e_dummy_`sector' // Independent var

			
			* Run the model according to type (fe, re, xtscc_fe, xtscc_re)
			
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
			
			e_base_year_dummy_sector `sector' fe
						
			* FE (Driscoll-Kraay)
			
			e_base_year_dummy_sector `sector' xtscc_fe

			* RE (cluster by events)
			
			e_base_year_dummy_sector `sector' re
			
			* RE (Driscoll-Kraay)
			
			e_base_year_dummy_sector `sector' xtscc_re
			
			* Report results using esttab
			
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "$tables_folder/e_robustness_analysis_m.tex", append ///
				b(%9.3f) se(%9.3f) stats(n_`sector' h_`sector' N_`sector', label("n" "$\bar{h}$" "N") fmt(%9.0f %9.0f %9.0f)) ///
				keep(_cons 1.e_dummy_`sector') ///
				varlabels(_cons "Intercepto" 1.e_dummy_`sector' "e-dummy") ///
				order(_cons) longtable ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				booktabs style(tex) nodepvars nomtitle ///
				posthead("\hline\multicolumn{5}{c}{\textit{`sector_name'}} \\ \hline") ///
				nonotes
		}
		
		
		
	/*----------------------
	First moment regression
	(z)
	________________________
	Paper and presentation
	version
	-----------------------*/
	
	* z_t(h) = c + \beta Dummy	
	*.........................................................................
	
	
	use z_affected_panel_cleaned, clear

	
		* Make the merge with the second dataset
		
		merge 1:1 target_date horizon using z_base_year_dummies_panel_cleaned

		
		* Check the result of the merge
		
		tab _merge

		
		* Remove remarks that do not appear in both datasets (optional)
		
		keep if _merge == 3

		
		* Delete the _merge variable (optional)
		
		drop _merge
			
			
		* Define the panel data structure
		
		xtset target_date horizon

		
		* Create macro for sectors
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
		
	
		* Clear any previous estimates
		
		estimates clear	
		
		
		* Program to run each regression for each sector
		
		
		program define z_base_year_dummy_sector
			args sector model_type
			
			local dep_var z_`sector' // Dependent var
			local indep_vars i.z_dummy_`sector' // Independent var

			
			* Run the model according to type (fe, re, xtscc_fe, xtscc_re)
			
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
			
			z_base_year_dummy_sector `sector' fe
						
			* FE (Driscoll-Kraay)
			
			z_base_year_dummy_sector `sector' xtscc_fe

			* RE (cluster by events)
			
			z_base_year_dummy_sector `sector' re
			
			* RE (Driscoll-Kraay)
			
			z_base_year_dummy_sector `sector' xtscc_re
			
			* Report results using esttab
			
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "$tables_folder/z_robustness_analysis_m.tex", append ///
				b(%9.3f) se(%9.3f) stats(n_`sector' h_`sector' N_`sector', label("n" "$\bar{h}$" "N") fmt(%9.0f %9.0f %9.0f)) ///
				keep(_cons 1.z_dummy_`sector') ///
				varlabels(_cons "Intercepto" 1.z_dummy_`sector' "z-dummy") ///
				order(_cons) longtable ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				booktabs style(tex) nodepvars nomtitle ///
				posthead("\hline\multicolumn{5}{c}{\textit{`sector_name'}} \\ \hline") ///
				nonotes
		}
		
	
	
	/*----------------------
	Drop aux data and tables
	-----------------------*/	

	
	* List all .dta, .txt and .tex files in the current directory and store in a local macro
	
	local dta_files : dir . files "*.dta"
	//local txt_files : dir . files "*.txt"
	//local tex_files : dir . files "*.tex"

	
	* Iterate over each .dta file and delete it
	
	foreach file of local dta_files {
		erase "`file'"
	}	
	
	
	* Iterate over each .txt file and delete it
	
	//foreach file of local txt_files {
	//	erase "`file'"
	//}	
	
	
	* Iterate over each .tex file and delete it
	
	//foreach file of local tex_files {
	//	erase "`file'"
	//}	
	
		