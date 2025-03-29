/********************
Master Do-file for Running All JEFAS Scripts
***

		Author
		---------------------
		Jason Cruz
		*********************/

	*** Program: jefas_master.do 
	**  First Created: 03/29/25 
	**  Last Updated:  03/29/25 
	
	***
	** Just click on the "Run (do)" button, the code will do the rest for you.
	***

	
	/*----------------------
	Initial script configuration
	-----------------------*/

	cls					// Clears the screen.
	clear all				// Frees all memory.
	version					// Displays the software version.
		
	set more off				// Turns off pagination for output.
	cap set maxvar 12000	// Sets the maximum number of variables to 12000.
	program drop _all		// Deletes all user-defined programs.
			
	capture log close		// Closes the log file if open.
				
	pause on				// Enables pauses in programs.
	set varabbrev off		// Turns off variable abbreviation.
		
	//log using jefas_unbiassdness.txt, text replace // Opens a log file and replaces it if it exists.

	

	/*----------------------
	Defining workspace path
	------------------------*/
	
	di "Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER."
	_request(path)
	cd "$path"


	/*----------------------
	Running all JEFAS do-files
	------------------------*/

	* Set global path for do-files (adjust accordingly)
	global dofile_path "$path"

	* Execute each do-file in order
	do "$dofile_path/jefas_e_unbiassdness"
	do "$dofile_path/jefas_e_unbiassdness_bench"
	do "$dofile_path/jefas_efficiency"
	do "$dofile_path/jefas_efficiency_bench"
	do "$dofile_path/jefas_efficiency_noconstant"
	do "$dofile_path/jefas_efficiency_noconstant_bench"
	do "$dofile_path/jefas_encompassing_noconstant"
	do "$dofile_path/jefas_min_zar"
	do "$dofile_path/jefas_min_zar_bench"
	do "$dofile_path/jefas_predictibility_noconstant"
	do "$dofile_path/jefas_predictibility_noconstant_bench"
	do "$dofile_path/jefas_r_unbiassdness"
	do "$dofile_path/jefas_r_unbiassdness_bench"

	di "All JEFAS do-files have been successfully executed."
	