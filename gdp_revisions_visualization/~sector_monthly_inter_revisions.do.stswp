/********************
Data Visualization for Monthly GDP Intermediate Revisions by sector
***

	Author
	---------------------
	Jason Cruz
	*********************/

	*** Program: sector_monthly_inter_revisions.do
	** 	First Created: 04/16/24
	** 	Last Updated:  04/--/24
			
				
/*----------------------
Initial script configuration
-----------------------*/

	cls
	clear all
	set more off
	cap set maxvar 12000
	program drop _all
	

	
/*----------------------
Defining auxiliary path
------------------------*/

	di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER  "'  _request(path)
	cd "$path"
	
	
/*----------------------
Setting folders to save outputs
------------------------*/
	
	shell mkdir output 		// to create folder to save outputs
	shell mkdir output/graphs // to create folder to save graphs
	
	* Define la ruta completa hacia la carpeta "graphs"
	
	global graphs_folder "output/graphs" // to export the graphics there
	
	
	
/*----------------------
Import ODBC dataset and
save temp
-----------------------*/

					
	odbc load, exec("select * from gdp_monthly_inter_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // When we use ODBC server

	save temp_data, replace
	
	
	
/*----------------------
Parsing and claning dataset
-----------------------*/
	

use temp_data, clear

	d // check entire dataset variables
		
		
	** Transform all variable names to lower case
	
	rename _all, lower
	
	
	** Vars label as vars names (lowercase)
	
	foreach var of varlist _all {
	label variable `var' "`var'"
	}
	
	
	** Check total observations 
	
	count // 148 observations
	
	
	** Generate monthly var and change format from %td to %tm
	
	gen monthly = mofd(inter_revision_date)
	format monthly %tm
	drop inter_revision_date
	rename monthly inter_revision_date // Return original var name
	
	
	** Order dataset
	
	order id inter_revision_date

save temp_data, replace



/*----------------------
Computing revision mean
by sector 
-----------------------*/
	

use temp_data, clear
	
	** Definir un nuevo sufijo para las variables promedio
	
	local sufijo _mean
	
	
	** Iterar sobre cada variable en la lista
	
	foreach var of varlist gdp_revision_* {
		// Calcular el promedio de la variable actual y guardar en una nueva variable con el sufijo "_mean"
		egen `var'`sufijo' = mean(`var')
	}
	
save temp_data, replace



/*----------------------
Charts for Monthly GDP
Revisions by sector
-----------------------*/
	
	
use temp_data, clear	
	

	** Global GDP Monthly Intermediate Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	"224 224 224" ///
	, n(3) nograph

	** Graph
	
	twoway (line gdp_revision_1 inter_revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line gdp_revision_2 inter_revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
		xtitle("", axis(1)) ///
		ytitle("GDP intermediate revisions") ///
		title("Global GDP Monthly Intermediate Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
		graphregion(color(white)) ///
		bgcolor(white) ///
		legend(position(1) label(1 "t+1") label(2 "t+2") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Horizon Legend (t+h)", size(*0.6)))
	
	** Export graph
	
	//graph export "${graphs_folder}/gdp_inter_revisions_m.pdf", as(pdf) replace
	//graph export "${graphs_folder}/gdp_inter_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/gdp_inter_revisions_m.png", as(png) replace



