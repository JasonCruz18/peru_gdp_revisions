/********************
Efficiency Cross-Releases
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: efficiency_cross_releases.do
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
		
		
	odbc load, exec("select * from sectorial_gdp_monthly_releases_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save merged_temp_panel_data, replace

	
	odbc load, exec("select * from sectorial_gdp_quarterly_cum_revisions_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save temp_data_e, replace
	
	
	odbc load, exec("select * from sectorial_gdp_quarterly_int_revisions_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save temp_data_r, replace
	
	
	
	/*----------------------
	On-the-fly data
	cleaning (1/3)
	-----------------------*/

	
	use merged_temp_panel_data, clear

	
		* Destring
		
		destring horizon, replace force
		
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
		
		* Filter observations starting from 1993m1
		keep if target_date >= ym(1993, 1)
		
		
		* Ordenar los datos por fecha y horizonte
		sort target_date horizon

		* Crear una variable con la predicción en h=1 (utilizando y_gdp)
		
		foreach sector of global sectors {
			* Crear la variable y_1_sector para cada sector, sólo si horizon == 1
			by target_date: gen y_1_`sector' = release_`sector' if horizon == 1
		}
		
		foreach sector of global sectors {
		* Propagar la predicción inicial a todas las filas del mismo target_date
			by target_date: replace y_1_`sector' = y_1_`sector'[_n-1] if missing(y_1_`sector')
		}
	
	save merged_temp_panel_data_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data
	cleaning (2/3)
	-----------------------*/

	
	use temp_data_e, clear
	
		* Paso 1: Dividir por 1000 para convertir de milisegundos a segundos
		gen vintages_seconds = vintages_date / 1000

		* Paso 2: Convertir los segundos a una fecha de Stata usando el formato %tc
		gen vintages_stata_date = vintages_seconds / 86400  // 86400 segundos en un día

		* Dar formato a la nueva variable
		format vintages_stata_date %td

		* Convertir la fecha de Stata a formato mensual
		gen vintages_monthly = mofd(vintages_stata_date)
		//gen vintages_quarterly = qofd(vintages_stata_date) // quarterly
		//gen vintages_annual = yofd(vintages_stata_date) // annual

		* Dar formato mensual
		format vintages_monthly %tm
		//format vintages_quarterly %tq // quarterly
		//format vintages_annual %ty // annual

	
	save temp_data_e_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data
	cleaning (3/3)
	-----------------------*/

	
	use temp_data_r, clear
	
		* Paso 1: Dividir por 1000 para convertir de milisegundos a segundos
		gen vintages_seconds = vintages_date / 1000

		* Paso 2: Convertir los segundos a una fecha de Stata usando el formato %tc
		gen vintages_stata_date = vintages_seconds / 86400  // 86400 segundos en un día

		* Dar formato a la nueva variable
		format vintages_stata_date %td

		* Convertir la fecha de Stata a formato mensual
		gen vintages_monthly = mofd(vintages_stata_date)
		//gen vintages_quarterly = qofd(vintages_stata_date) // quarterly
		//gen vintages_annual = yofd(vintages_stata_date) // annual

		* Dar formato mensual
		format vintages_monthly %tm
		//format vintages_quarterly %tq // quarterly
		//format vintages_annual %ty // annual

	
	save temp_data_r_cleaned, replace
	
	
		
	/*----------------------
	Regression (revisions-releases)
	-----------------------*/

	* r_t(h) vs y_t(h-1) y_t(h-2)	
	*.........................................................................
	
	
	use merged_temp_panel_data_cleaned, clear
	preserve // Save the current status
	
	
		/* Limpiar cualquier estimación previa */
		estimates clear	
		
		/* Programa para procesar cada sector */
		program define process_release_sector_r
			args sector model_type
			local dep_var r_`sector'
			local indep_vars L1.release_`sector' L2.release_`sector'
			
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
			process_release_sector_r `sector' fe
			
			/* Test sobre las restricciones */
			test (L1.release_`sector' = 0) (L2.release_`sector' = 0)
			
			/* Guardar los valores de F y p */
			scalar chi_value = 2*r(F) // Don't report chi2
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value
			
			/* Correr regresión de efectos fijos con Driscoll-Kraay */
			process_release_sector_r `sector' xtscc_fe

			/* Test sobre las restricciones */
			test (L1.release_`sector' = 0) (L2.release_`sector' = 0)
			
			/* Guardar los valores de F y p */
			scalar chi_value = 2*r(F) // Don't report chi2
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value

			/* Correr regresión de efectos aleatorios */
			process_release_sector_r `sector' re
			
			/* Test sobre las restricciones */
			test (L1.release_`sector' = 0) (L2.release_`sector' = 0)
			
			/* Guardar los valores de F y p */
			scalar chi_value = r(chi2)
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value

			/* Correr regresión de efectos aleatorios con Driscoll-Kraay */
			process_release_sector_r `sector' xtscc_re
			
			/* Test sobre las restricciones */
			test (L1.release_`sector' = 0) (L2.release_`sector' = 0)
			
			/* Guardar los valores de F y p */
			scalar chi_value = 2*r(F) // Alternatively e(chi2) 
			scalar p_value = r(p)
			estadd scalar chi_`sector' chi_value
			estadd scalar p_`sector' p_value
			
			/* Reportar los resultados usando esttab */
			esttab fe_`sector' xtscc_fe_`sector' re_`sector' xtscc_re_`sector' using "predictibility_releases.tex", append ///
				b(%9.3f) se(%9.3f) stats(chi_`sector' p_`sector' n_`sector' h_`sector' N_`sector', label("Chi2" "p" "n" "h" "N") fmt(%9.3f %9.3f %9.0f %9.0f %9.0f)) ///
				order(_cons) ///
				varlabels(_cons "Intercepto" L.release_`sector' "y(-1)" L2.release_`sector' "y(-2)") ///
				noobs ///
				star(* 0.1 ** 0.05 *** 0.01) ///
				tex ///
				longtable
		}
	
	
	restore // Return to on-call status
	

	
	/*----------------------
	Regression (revision-first-release: e)
	-----------------------*/

	* e_t(h) vs y_t(h=1)	
	*.........................................................................
	
	use temp_data_e_cleaned, clear

		tsset vintages_monthly

		preserve // Guardar el estado actual

		/* Limpiar cualquier estimación previa */
		estimates clear

		/* Programa para procesar cada sector */
		program define process_e
			args dep_var indep_var model_type sector
			/* Ejecutar el modelo Newey-West */
			if "`model_type'" == "newey" {
				newey `dep_var' `indep_var', lag(1) force
			}

			/* Guardar número total de observaciones */
			scalar obs_total = e(N)
			estadd scalar N_`sector' = obs_total

			/* Guardar los resultados del modelo */
			estimates store `model_type'_`sector'
		end

		/* Número de releases */
		local num_releases 10

		/* Loop para correr las regresiones para cada sector y releases */
		foreach sector of global sectors {
			forval i = 1/`num_releases' {
				/* Construir nombres de variables dependientes e independientes */
				local dep_var e_`i'_`sector'
				local indep_var `sector'_release_1

				/* Regresión Newey-West */
				process_e `dep_var' `indep_var' newey `sector'

				/* Test sobre las restricciones */
				//test (_cons = 0) (`indep_var' = 0)

				/* Guardar los valores F y p */
				//scalar f_value = r(F)
				//scalar p_value = r(p)
				//estadd scalar F_`sector'_release`i'_newey = f_value
				//estadd scalar p_`sector'_release`i'_newey = p_value

				/* Exportar resultados a LaTeX */
				esttab newey_`sector' using "predictibility_first_release_e.tex", append ///
					b(%9.3f) se(%9.3f) stats(N_`sector', ///
					label("n") fmt(%9.3f %9.3f %9.0f)) ///
					keep(_cons `indep_var') ///
					order(_cons `indep_var') ///
					varlabels(_cons "Intercepto" `indep_var' "Primera Predicción") ///
					noobs star(* 0.1 ** 0.05 *** 0.01) tex longtable
			}
		}


	restore // Restaurar dataset original

	
	
	/*----------------------
	Regression (revision-first-release: r)
	-----------------------*/

	* r_t(h) vs y_t(h=1)	
	*.........................................................................
	
	use temp_data_r_cleaned, clear

		tsset vintages_monthly

		preserve // Guardar el estado actual

		/* Limpiar cualquier estimación previa */
		estimates clear

		/* Programa para procesar cada sector */
		program define process_r
			args dep_var indep_var model_type sector
			/* Ejecutar el modelo Newey-West */
			if "`model_type'" == "newey" {
				newey `dep_var' `indep_var', lag(1) force
			}

			/* Guardar número total de observaciones */
			scalar obs_total = e(N)
			estadd scalar N_`sector' = obs_total

			/* Guardar los resultados del modelo */
			estimates store `model_type'_`sector'
		end

		/* Número de releases */
		local num_releases 10

		/* Loop para correr las regresiones para cada sector y releases */
		foreach sector of global sectors {
			forval i = 2/`num_releases' {
				/* Construir nombres de variables dependientes e independientes */
				local dep_var r_`i'_`sector'
				local indep_var `sector'_release_1

				/* Regresión Newey-West */
				process_r `dep_var' `indep_var' newey `sector'

				/* Test sobre las restricciones */
				//test (_cons = 0) (`indep_var' = 0)

				/* Guardar los valores F y p */
				//scalar f_value = r(F)
				//scalar p_value = r(p)
				//estadd scalar F_`sector'_release`i'_newey = f_value
				//estadd scalar p_`sector'_release`i'_newey = p_value

				/* Exportar resultados a LaTeX */
				esttab newey_`sector' using "predictibility_first_release_r.tex", append ///
					b(%9.3f) se(%9.3f) stats(N_`sector', ///
					label("n") fmt(%9.3f %9.3f %9.0f)) ///
					keep(_cons `indep_var') ///
					order(_cons `indep_var') ///
					varlabels(_cons "Intercepto" `indep_var' "Primera Predicción") ///
					noobs star(* 0.1 ** 0.05 *** 0.01) tex longtable
			}
		}


	restore // Restaurar dataset original
	
	
	
	/*----------------------
	Drop aux data and tables
	-----------------------*/	

		// List all .dta, .txt and .tex files in the current directory and store in a local macro
		local dta_files : dir . files "*.dta"
		//local txt_files : dir . files "*.txt"
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
		
		