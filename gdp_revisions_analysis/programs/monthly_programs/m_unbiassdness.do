/********************
Summary of Statistics
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: unbiassdness.do
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
		
		
	odbc load, exec("select * from sectorial_gdp_annual_cum_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save cum_ts_data, replace
	
	
	odbc load, exec("select * from sectorial_gdp_annual_int_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save int_ts_data, replace
	
	
	
	/*----------------------
	e: summary of stats
	(mean with significance)
	-----------------------*/

	
	use cum_ts_data, clear
	preserve // Save the current status
	
		
		frame create results_e str32 variable int n str32 coef str8 sd str8 p1 str8 p99

		foreach sector in $sectors {
			
			* Recorre las variables del tipo e_#_`sector` y realiza las regresiones
			forval i = 1/19 {
				capture confirm variable e_`i'_`sector'
				if !_rc {
					capture {
						regress e_`i'_`sector'
						if _rc == 2001 {
							di in red "Insufficient observations for e_`i'_`sector'"
							continue
						}
						matrix M = r(table)
						
						* Calcula estadísticas detalladas
						summarize e_`i'_`sector', detail
						local n = r(N)
						local sd = string(r(sd), "%9.3f")
						local p1 = string(r(p1), "%9.3f")
						local p99 = string(r(p99), "%9.3f")
						
						local coef = M["b", "_cons"]
						local pvalue = M["pvalue", "_cons"]
						
						if `pvalue' < 0.01 {
							local coef = string(`coef', "%9.3f") + "***"
						}
						else if `pvalue' >= 0.01 & `pvalue' < 0.05 {
							local coef = string(`coef', "%9.3f") + "**"
						}
						else if `pvalue' >= 0.05 & `pvalue' < 0.10 {
							local coef = string(`coef', "%9.3f") + "*"
						}
						else {
							local coef = string(`coef', "%9.3f")
						}
						
						frame post results_e ("e_`i'_`sector'") (`n') ("`coef'") ("`sd'") ("`p1'") ("`p99'")
					}
				}
				else {
					di in yellow "Variable e_`i'_`sector' does not exist"
				}
			}
		}

		* Cambia al marco de resultados y lista los datos
		frame change results_e
		//list n coef p1 p99 sd, noobs clean
		list variable n coef p1 p99 sd, noobs clean
	
	
	restore // Return to on-call status
	
		
	
	/*----------------------
	r: summary stats
	(mean with significance)
	
	-----------------------*/
	
		
	use int_ts_data, clear
	preserve // Save the current status
	
	
		frame create results_r str32 variable int n str32 coef str8 sd str8 p1 str8 p99

		foreach sector in $sectors {
			
			* Recorre las variables del tipo r_#_`sector` y realiza las regresiones
			forval i = 2/20 {
				capture confirm variable r_`i'_`sector'
				if !_rc {
					capture {
						regress r_`i'_`sector'
						if _rc == 2001 {
							di in red "Insufficient observations for r_`i'_`sector'"
							continue
						}
						matrix M = r(table)
						
						* Calcula estadísticas detalladas
						summarize r_`i'_`sector', detail
						local n = r(N)
						local sd = string(r(sd), "%9.3f")
						local p1 = string(r(p1), "%9.3f")
						local p99 = string(r(p99), "%9.3f")
						
						local coef = M["b", "_cons"]
						local pvalue = M["pvalue", "_cons"]
						
						if `pvalue' < 0.01 {
							local coef = string(`coef', "%9.3f") + "***"
						}
						else if `pvalue' >= 0.01 & `pvalue' < 0.05 {
							local coef = string(`coef', "%9.3f") + "**"
						}
						else if `pvalue' >= 0.05 & `pvalue' < 0.10 {
							local coef = string(`coef', "%9.3f") + "*"
						}
						else {
							local coef = string(`coef', "%9.3f")
						}
						
						frame post results_r ("r_`i'_`sector'") (`n') ("`coef'") ("`sd'") ("`p1'") ("`p99'")
					}
				}
				else {
					di in yellow "Variable r_`i'_`sector' does not exist"
				}
			}
		}

		* Cambia al marco de resultados y lista los datos
		frame change results_r
		//list n coef p1 p99 sd, noobs clean
		list variable n coef p1 p99 sd, noobs clean
			
					
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
		
		