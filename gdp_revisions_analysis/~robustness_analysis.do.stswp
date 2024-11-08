/********************
Robustness Analysis
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: robustness_analysis.do
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
		
		
	odbc load, exec("select * from sectorial_gdp_monthly_cum_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save cum_temp_panel_data, replace
	
	
	odbc load, exec("select * from sectorial_gdp_monthly_int_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save int_temp_panel_data, replace
	
	
	odbc load, exec("select * from sectorial_gdp_monthly_int_revisions_panel_dummies_base_year") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save temp_dummies_data, replace
	
	
	
	/*----------------------
	On-the-fly data
	cleaning (1/3)
	-----------------------*/

	
	use cum_temp_panel_data, clear

	
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
	
	
	save cum_temp_panel_data_cleaned, replace
	
	

	/*----------------------
	On-the-fly data
	cleaning (2/3)
	-----------------------*/

	
	use int_temp_panel_data, clear

	
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
	
	
	save int_temp_panel_data_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data
	cleaning (3/3)
	-----------------------*/

	
	use temp_dummies_data, clear

	
		* Order and sort
		
		sort vintages_date horizon // Key step to set both the ID and time vars for panel data.

		
		* Fixing date format
		
		gen numeric_date = dofc(vintages_date) // To a Stata date in days.
		format numeric_date %td // To standard Stata date (e.g., day-month-year).

		gen target_date = mofd(numeric_date) // To a monthly date format.
		format target_date %tm // To standard Stata month (e.g., Jan 2023).

		drop vintages_date numeric_date // Drop the original vars since they are no longer needed.

		order target_date horizon // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.

	
	save temp_dummies_data_cleaned, replace
	
	
	
	/*----------------------
	Regression (base year
	benchmark revisions)
	-----------------------*/
	
	* r_t(h) vs _cons _consxDummy	
	*.........................................................................
	
	
	* Cargar el primer dataset
	use int_temp_panel_data_cleaned, clear

	
		* Hacer el merge con el segundo dataset
		merge 1:1 target_date horizon using temp_dummies_data_cleaned

		* Revisar el resultado del merge
		tab _merge

		* Eliminar observaciones que no aparezcan en ambos datasets (opcional)
		keep if _merge == 3

		* Eliminar la variable _merge (opcional)
		drop _merge
			
		/* Definir la estructura de datos de panel */
		xtset target_date horizon
		
		** Generate constant varabbrev
		
		gen constant = 1

		* global
		
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services
		
	
		/* Limpiar cualquier estimación previa */
		estimates clear	
		
		/* Programa para procesar cada sector */
		program define base_year_dummy_process_sector_r
			args sector model_type
			local dep_var r_`sector'
			local indep_vars 

			
			/* Ejecutar el modelo según el tipo (fe, re, xtscc) */
			if "`model_type'" == "fe" {
				xtreg `dep_var' `indep_vars', fe vce(cluster target_date)
			}
			else if "`model_type'" == "re" {
				xtreg `dep_var' `indep_vars', re vce(cluster target_date)
			}
			else if "`model_type'" == "xtscc_fe" {
				xtscc `dep_var' `indep_vars', fe
			}
			else if "`model_type'" == "xtscc_re" {
				xtscc `dep_var' `indep_vars', re
			}
			
			/* Obtener las dimensiones del panel con xtsum */
			xtsum `dep_var'
			scalar obs_between = r(n)     // Número de grupos (between)
			scalar obs_within = r(Tbar)   // Número de periodos promedio por grupo (within)
			scalar obs_total = r(N)       // Número total de observaciones (overall)
			
			/* Agregar los valores de observaciones a los resultados */
			estadd scalar n_`sector' obs_between
			estadd scalar h_`sector' obs_within
			estadd scalar N_`sector' obs_total
			
			/* Guardar los resultados del modelo */
			estimates store `model_type'_`sector'
		end

		/* Loop para correr las regresiones para cada sector */
		foreach sector of global sectors {
			
			/* Correr regresión de efectos fijos */
			base_year_dummy_process_sector_r `sector' fe
						
			/* Correr regresión de efectos fijos con Driscoll-Kraay */
			base_year_dummy_process_sector_r `sector' xtscc_fe

			/* Correr regresión de efectos aleatorios */
			base_year_dummy_process_sector_r `sector' re
			
			/* Correr regresión de efectos aleatorios con Driscoll-Kraay */
			base_year_dummy_process_sector_r `sector' xtscc_re
			
			/* Reportar los resultados usando esttab */
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "robustness_analysis.txt", append ///
				b(%9.3f) se(%9.3f) stats(n_`sector' h_`sector' N_`sector', label("n" "h" "N") fmt(%9.0f %9.0f %9.0f)) ///
				order(_cons) ///
				varlabels(_cons "Intercepto") ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01)
		}
	
	
	
	/*----------------------
	Drop aux data and tables
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