/********************
Summary of Statistics
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: stats_sum.do
		** 	First Created: 10/28/24
		** 	Last Updated:  11/##/24
			
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
		
		
	odbc load, exec("select * from sectorial_gdp_monthly_cum_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save cum_ts_data, replace
	
	
	odbc load, exec("select * from sectorial_gdp_monthly_int_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save int_ts_data, replace
	
	
	
	/*----------------------
	Summary of statistics
	(nowcast errors)
	-----------------------*/

	
	use cum_ts_data, clear
	preserve // Save the current status
	
		* // ssc install estout
		

		// Define los sectores como una macro global
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services

		// Inicializa una tabla vacía
		eststo clear

		// Bucle para calcular los estadísticos y almacenarlos
		foreach sector in $sectors {
			// Calcula los estadísticos para cada sector
			estpost tabstat e_*_`sector', statistics(mean p1 p25 p50 p75 p99 sd) columns(statistics)
			
			// Almacena los resultados
			eststo `sector'
			
			// Exporta los resultados a LaTeX, asegurando que estén bajo las mismas columnas
		esttab `sector' using "stats_sum_e.tex", append label cells("mean(fmt(%9.3f) star) p1(fmt(3)) p25(fmt(3)) p50(fmt(3)) p75(fmt(3)) p99(fmt(3)) sd(fmt(3))") ///
			longtable ///
			tex ///
			title("Estadísticos Descriptivos") ///
			varwidth(15) ///
			not noobs
		}


		* Unbiassdness (e)
		*.....................................................................
		
		// Inicializa una tabla vacía
		eststo clear
		
		* Definir el archivo de salida
		local output_file "unbiassdness_e.txt"

		* Borrar el archivo de salida si ya existe
		capture erase `output_file'

		foreach sector in $sectors {
			* Crear lista de variables para el sector
			local varlist
			
			* Recorre las variables del tipo e_#_`sector` y realiza las regresiones
			forval i = 1/20 {
				capture confirm variable e_`i'_`sector'
				if !_rc {
					* Realizar regresión y guardar el coeficiente de la constante
					regress e_`i'_`sector'
					
					* Guardar el coeficiente de la constante (_cons) en una lista
					local cons_coef `cons_coef' _b[_cons]
					
					* Guardar los resultados en el archivo con append
					esttab using `output_file', append ///
						title("unbiassdness (e)") ///
						varwidth(15) ///
						not noobs
				}
				else {
					* Cuando la variable no exista, salimos del bucle
					break
				}
			}
		}


	restore // Return to on-call status
	
	
	
	/*----------------------
	Summary of statistics
	(revisions)
	-----------------------*/
	
		
	use int_ts_data, clear
	preserve // Save the current status
	
	
		// Define los sectores como una macro global
		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services

		// Inicializa una tabla vacía
		eststo clear

		// Bucle para calcular los estadísticos y almacenarlos
		foreach sector in $sectors {
			// Calcula los estadísticos para cada sector
			estpost tabstat r_*_`sector', statistics(mean p1 p25 p50 p75 p99 sd) columns(statistics)
			
			// Almacena los resultados
			eststo `sector'
			
			// Exporta los resultados a LaTeX, asegurando que estén bajo las mismas columnas
		esttab `sector' using "stats_sum_r.tex", append label cells("Mean(fmt(3)) p1(fmt(3)) p25(fmt(3)) p50(fmt(3)) p75(fmt(3)) p99(fmt(3)) SD(fmt(3))") ///
			varlabels(r_*_* "h") ///
			longtable ///
			title("Estadísticos Descriptivos") ///
			varwidth(15) ///
			tex ///
			not noobs
		}


		* Unbiassdness (r)
		*.....................................................................
		
		// Inicializa una tabla vacía
		eststo clear
		
		* Definir el archivo de salida
		local output_file "unbiassdness_r.txt"

		* Borrar el archivo de salida si ya existe
		capture erase `output_file'

		foreach sector in $sectors {
			* Crear lista de variables para el sector
			local varlist
			
			* Recorre las variables del tipo r_#_`sector` y realiza las regresiones
			forval i = 1/20 {
				capture confirm variable r_`i'_`sector'
				if !_rc {
					* Realizar regresión y guardar el coeficiente de la constante
					regress r_`i'_`sector'
					
					* Guardar el coeficiente de la constante (_cons) en una lista
					local cons_coef `cons_coef' _b[_cons]
					
					* Guardar los resultados en el archivo con append
					esttab using `output_file', append ///
						title("unbiassdness (r)") ///
						varwidth(15) ///
						not noobs
				}
				else {
					* Cuando la variable no exista, salimos del bucle
					break
				}
			}
		}

		
	restore // Return to on-call status
	
	
	
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
		
		