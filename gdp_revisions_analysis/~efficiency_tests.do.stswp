/********************
Efficiency Tests
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: efficiency_tests.do
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
	//shell mkdir "output/data" 		// Creates folder to save data.
		
	
	* Set as global vars
	
	//global graphs_folder "output/graphs"	// Use to export graphs.
	global tables_folder "output/tables"	// Use to export tables.
	//global data_folder "output/data"		// Use to export .dta.
	
		
		
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
		
		
	odbc load, exec("select * from sectorial_gdp_monthly_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save temp_panel_data, replace


	
	/*----------------------
	On-the-fly data cleaning
	-----------------------*/

	
	use temp_panel_data, clear

	
		* Order and sort
		
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

		
		/* Definir la estructura de datos de panel */
		xtset target_date horizon

		
		* global
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
	
	
	
	/*----------------------
	 Regression
	 (nowcast error)
	 ----------------------*/

		
		/* Limpiar cualquier estimación previa */
		estimates clear
		
		/* Loop para correr las regresiones */
		foreach sector of global sectors {
			
			/* Correr la regresión de efectos fijos */
			xtreg e_`sector' L1.r_`sector' L2.r_`sector', fe vce(cluster target_date)
			
			estimates store FEC_`sector'  // Guardar los resultados de FE
			
			/* Correr la regresión de efectos fijos DK */
			// ssc install xtscc
			xtscc e_`sector' L1.r_`sector' L2.r_`sector', fe
			
			estimates store FEDK_`sector'  // Guardar los resultados de FE
			
			/* Correr la regresión de efectos aleatorios */
			xtreg e_`sector' L1.r_`sector' L2.r_`sector', re vce(cluster target_date)
					
			estimates store REC_`sector'  // Guardar los resultados de RE
			
			/* Correr la regresión de efectos fijos DK*/
			// ssc install xtscc
			xtscc e_`sector' L1.r_`sector' L2.r_`sector', re
			
			estimates store REDK_`sector'  // Guardar los resultados de FE
			
			/* Reportar resultados usando esttab */
			/* Esttab para mostrar coeficientes, intercepto y test F */
			esttab FEC_`sector' FEDK_`sector' REC_`sector' REDK_`sector' using "error.tex", append ///
				b(%9.3f) se(%9.3f) scalars(F chi2 p) ///
				order(_cons) longtable ///
				varlabels(_cons "Intercepto" L.r_`sector' "r(-1)" L2.r_`sector' "r(-2)") ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				tex
		}
	
			
		
	/*----------------------
	Regression (intermediate
	revisions)
	-----------------------*/
		
		
		/* Limpiar cualquier estimación previa */
		estimates clear
		
		/* Loop para correr las regresiones */
		foreach sector of global sectors {
			
			/* Correr la regresión de efectos fijos */
			xtreg r_`sector' L1.r_`sector' L2.r_`sector', fe vce(cluster target_date)
			
			estimates store FEC_`sector'  // Guardar los resultados de FE
			
			/* Correr la regresión de efectos fijos DK */
			// ssc install xtscc
			xtscc r_`sector' L1.r_`sector' L2.r_`sector', fe
			
			estimates store FEDK_`sector'  // Guardar los resultados de FE
			
			/* Correr la regresión de efectos aleatorios */
			xtreg r_`sector' L1.r_`sector' L2.r_`sector', re vce(cluster target_date)
					
			estimates store REC_`sector'  // Guardar los resultados de RE
			
			/* Correr la regresión de efectos fijos DK*/
			// ssc install xtscc
			xtscc r_`sector' L1.r_`sector' L2.r_`sector', re
			
			estimates store REDK_`sector'  // Guardar los resultados de FE
			
			/* Reportar resultados usando esttab */
			/* Esttab para mostrar coeficientes, intercepto y test F */
			esttab FEC_`sector' FEDK_`sector' REC_`sector' REDK_`sector' using "revision.tex", append ///
				b(%9.3f) se(%9.3f) scalars(F chi2 p) ///
				order(_cons) longtable ///
				varlabels(_cons "Intercepto" L.r_`sector' "r(-1)" L2.r_`sector' "r(-2)") ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				tex
		}
	
	
	
	/*----------------------
	Drop aux data (.dta)
	-----------------------*/	

		// List all .dta, .txt and .tex files in the current directory and store in a local macro
		local dta_files : dir . files "*.dta"
		local txt_files : dir . files "*.txt"
		local tex_files : dir . files "*.tex"

		// Iterate over each .dta file and delete it
		foreach file of local dta_files {
			erase "`file'"
		}	
		
		// Iterate over each .txt file and delete it
		foreach file of local txt_files {
			erase "`file'"
		}	
		
		// Iterate over each .tex file and delete it
		foreach file of local tex_files {
			erase "`file'"
		}	
	
	