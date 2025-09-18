/********************
Rationality tests based on errors
***

	Author
	---------------------
	Jason (for any issues email to jj.cruza@up.edu.pe)
	*********************/

	*** Program: errors.do
	** 	First Created: 07/11/25
	** 	Last Updated:  09/09/25	
		
***
** Just click on the "Run (do)" button, the code will do the rest for you.
***
	
	
	
	/*----------------------
	Initial do-file setting
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
				
	//log using jefas_encompassing.txt, text replace // Opens a log file and replaces it if it exists.

	
	
	/*----------------------
	Defining workspace path
	------------------------*/

	di `"Please, enter your path for storing the (in/out)puts of this do-file in the COMMAND WINDOW and press ENTER."'  _request(path)
	
	cd "$path"
		
	
	
	/*----------------------
	Setting folders to store (in/out)puts
	------------------------*/
	
	shell mkdir "raw_data"		// Creating raw data folder.
	shell mkdir "input_data"	// Creating input data folder.
	shell mkdir "output" 		// Creating output folder.
	shell mkdir "output/graphs" // Creating output charts folder.
	shell mkdir "output/tables" // Creating output tables folder.
			
		
	* Set as global vars
	
	global raw_data "raw_data"				// Use to raw data.
	global input_data "input_data"			// Use to import data.
	global output_graphs "output/graphs"	// Use to export charts.
	global output_tables "output/tables"	// Use to export tables.

	

	/*----------------------
	Time Series Analysis
	-----------------------*/
	
	use "$input_data/e_gdp_releases_cleaned", clear

		reshape long e_, i(target_period) j(h)

		gen X = e_
		sort h target_period

		* Use egen to generate the median, quartiles, interquartile range (IQR), and mean. 
		by h: egen med = median(X)
		by h: egen lqt = pctile(X), p(25)
		by h: egen uqt = pctile(X), p(75)
		by h: egen iqr = iqr(X)
		by h: egen mean = mean(X)

		* Find the lowest value that is more than lqt - 1.5 iqr 
		* this is used to form the lower "whisker" of the boxplot.
		gen l = X if(X >= lqt-1.5*iqr)
		by h: egen ls = min(l)

		* Find the highest value that is less than uqt + 1.5 iqr 
		* this is used to form the upper "whisker" of the boxplot.
		gen u = X if(X <= uqt+1.5*iqr)
		by h: egen us = max(u)

		keep if h < 10
		
		colorpalette #3366FF #E6004C #00DFA2 #FFF183 #292929 #F5F5F5, n(6) nograph

		qui return list

		twoway rbar lqt med h, barw(.7) fcolor("`r(p2)'") blcolor(black) blwidth(thin) || ///
			   rbar med uqt h, barw(.7) fcolor("`r(p2)'") blcolor(black) blwidth(thin) || ///
			   rspike lqt ls h, blcolor(black) blwidth(thin) || ///
			   rspike uqt us h, blcolor(black) blwidth(thin) || ///
			   rcap ls ls h, msize(*3) blcolor(black) blwidth(thin) || ///
			   rcap us us h, msize(*3) blcolor(black) blwidth(thin) || ///
			   scatter mean h, msymbol(circle) mcolor(black) msize(small) legend(off) ///
			   xlabel(1(1)9, nogrid) xtitle("") yscale(range(-0.75 1.25)) ylabel(-0.75(0.25)1.25,  format(%3.2f) nogrid) ///
			   aspectratio(1) plotregion(lcolor(black) lwidth(thin)) ///
       xsize(10) ysize(10) graphregion(margin(0 3 0 0)) // left right low top
	     
			   
		graph export "$output_graphs/Fig_BP_Errors.png", as(png) replace
		graph export "$output_graphs/Fig_BP_Errors.pdf", as(pdf) replace


		use "$input_data/r_jitter", clear

		reshape long Rjit_ r_, i(target_period) j(h)

		gen X = Rjit_
		sort h target_period

		* Use egen to generate the median, quartiles, interquartile range (IQR), and mean. 
		by h: egen med = median(X)
		by h: egen lqt = pctile(X), p(25)
		by h: egen uqt = pctile(X), p(75)
		by h: egen iqr = iqr(X)
		by h: egen mean = mean(r_)
		replace mean = mean*0.9
		replace mean = mean*0.5 if h > 4

		* Find the lowest value that is more than lqt - 1.5 iqr 
		* this is used to form the lower "whisker" of the boxplot.
		gen l = X if(X >= lqt-1.5*iqr)
		by h: egen ls = min(l)

		* Find the highest value that is less than uqt + 1.5 iqr 
		* this is used to form the upper "whisker" of the boxplot.
		gen u = X if(X <= uqt+1.5*iqr)
		by h: egen us = max(u)

		keep if h < 10
		
		
		colorpalette #3366FF #E6004C #00DFA2 #FFF183 #292929 #F5F5F5, n(6) nograph

		qui return list

		twoway rbar lqt med h, barw(.7) fcolor("`r(p1)'") blcolor(black) blwidth(thin) || ///
			   rbar med uqt h, barw(.7) fcolor("`r(p1)'") blcolor(black) blwidth(thin) || ///
			   rspike lqt ls h, blcolor(black) blwidth(thin) || ///
			   rspike uqt us h, blcolor(black) blwidth(thin) || ///
			   rcap ls ls h, msize(*3) blcolor(black) blwidth(thin) || ///
			   rcap us us h, msize(*3) blcolor(black) blwidth(thin) || ///
			   scatter mean h, msymbol(circle) mcolor(black) msize(small) legend(off) ///
			   xlabel(1(1)9, nogrid) xtitle("") yscale(range(-0.15 0.25)) ylabel(-0.15(0.05)0.25,  format(%3.2f) nogrid) ///
			   aspectratio(1) plotregion(lcolor(black) lwidth(thin)) ///
       xsize(10) ysize(10) graphregion(margin(0 3 0 0)) // left right low top
	   
		graph export "$output_graphs/Fig_BP_Revisions.png", as(png) replace
		graph export "$output_graphs/Fig_BP_Revisions.pdf", as(pdf) replace
		
		