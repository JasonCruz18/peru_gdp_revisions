/********************
Predictibility (e)
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: m_predictibility_e.do
		** 	First Created: 12/03/24
		** 	Last Updated:  12/--/24
			
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
	
	
	odbc load, exec("select * from r_sectorial_gdp_monthly_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save r_panel, replace
	
	
	odbc load, exec("select * from e_sectorial_gdp_monthly_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save e_panel, replace
	
	

	/*----------------------
	On-the-fly data cleaning
	(r)
	-----------------------*/

	
	use r_panel, clear

	
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
	
	
	save r_panel_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(e)
	-----------------------*/

	
	use e_panel, clear

	
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
	
	
	save e_panel_cleaned, replace
	
	
	
	/*----------------------
	Merge e with z
	-----------------------*/
	
	
	use r_panel_cleaned, clear
	

		* Merge with the second dataset (e_panel_cleaned)
		
		merge 1:1 target_date horizon using e_panel_cleaned

		
		* Check the merge result
		
		tab _merge // _merge values: 1 = only in master, 2 = only in using, 3 = matched

		* If you want to keep only the matches:
		
		keep if _merge == 3
		drop _merge
		

	save r_e_panel, replace

		
		
	/*----------------------
	Predictibility (e)
	________________________
	Paper and presentation
	version
	-----------------------*/

	* e_t(h) = c + \beta_1 r_t(h-1) + \beta_2 r_t(h-2)	
	*.........................................................................
	
	
	use r_e_panel, clear
	preserve // Save the current status
	
	
		* Define the panel data structure
		
		xtset target_date horizon

		
		* Create macro for sectors
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
	
	
		* Clear any previous estimates
		
		estimates clear	
		
		
		* Program to run each regression for each sector
		
		program define e_predictibility_sector
			args sector model_type
			
			local dep_var r_`sector' // Dependent var
			local indep_vars L1.r_`sector' L2.r_`sector' // Independent var
			
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
			
			* Store the values of F and p 
			
			*scalar F_value = r(F)
			*scalar p_value = r(p)
			*estadd scalar F_`sector' F_value
			*estadd scalar p_`sector' p_value
			
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
			
			e_predictibility_sector `sector' fe
			
			* Test on restrictions
			
			test (L1.r_`sector' = 0) (L2.r_`sector' = 0)
			
			* Store the values of F and p 
			
			scalar chi_value = 2*r(F) // Don't report chi2
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value
			
			* FE (Driscoll-Kraay)
			
			e_predictibility_sector `sector' xtscc_fe

			* Test on restrictions
			
			test (L1.r_`sector' = 0) (L2.r_`sector' = 0)
			
			* Store the values of F and p 
			
			scalar chi_value = 2*r(F) // Don't report chi2
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value

			* RE (cluster by events)
			
			e_predictibility_sector `sector' re
			
			* Test on restrictions
			
			test (L1.r_`sector' = 0) (L2.r_`sector' = 0)
			
			* Store the values of F and p 
			
			scalar chi_value = r(chi2)
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value

			* RE (Driscoll-Kraay)
			
			e_predictibility_sector `sector' xtscc_re
			
			* Test on restrictions
			
			test (L1.r_`sector' = 0) (L2.r_`sector' = 0)
			
			* Store the values of F and p 
			
			scalar chi_value = 2*r(F) // Alternatively e(chi2) 
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value
			
			* Report results using esttab
			
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "$tables_folder/e_predictibility_sector_m.tex", append ///
				b(%9.3f) se(%9.3f) stats(chi_`sector' p_`sector' n_`sector' h_`sector' N_`sector', label("Chi2" "p-value" "n" "$\bar{h}$" "N") fmt(%9.3f %9.3f %9.0f %9.0f %9.0f)) ///
				order(_cons) longtable ///
				varlabels(_cons "Intercepto" L.r_`sector' "r(-1)" L2.r_`sector' "r(-2)") ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				booktabs style(tex) nodepvars nomtitle ///
				posthead("\hline\multicolumn{5}{c}{\textit{`sector_name'}} \\ \hline") ///
				nonotes
		}
	
	
	restore // Return to on-call status
	
		
	
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
	
	