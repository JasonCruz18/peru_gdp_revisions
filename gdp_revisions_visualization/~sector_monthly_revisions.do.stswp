/********************
Data Visualization for Monthly GDP Revisions by sector
***

	Author
	---------------------
	Jason Cruz
	*********************/

	*** Program: sector_monthly_revisions.do
	** 	First Created: 04/10/24
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

					
	odbc load, exec("select * from sector_monthly_revision") dsn("gdp_revisions_datasets") lowercase sqlshow clear // When we use ODBC server

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
	
	** Order dataset
	
	order id revision_date

save temp_data, replace
	
	
	
/*----------------------
Ploting monthly GDP
revisions by sector
-----------------------*/
	
	
use temp_data, clear	
	

	** Global GDP Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	** Plotting
	
	twoway (line gdp_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
		xtitle("", axis(1)) ///
		ytitle("GDP revisions") ///
		title("Global GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
		graphregion(color(white)) ///
		bgcolor(white)
	
	** Export graph
	
	graph export "${graphs_folder}/gdp_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/gdp_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/gdp_revisions_m.png", as(png) replace
	
	
	** Agriculture GDP Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line agriculture_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Agriculture GDP revisions") ///
	title("Agriculture GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/agriculture_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/agriculture_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/agriculture_revisions_m.png", as(png) replace

	
	**  Commerce Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line commerce_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Commerce GDP revisions") ///
	title("Commerce GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/commerce_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/commerce_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/commerce_revisions_m.png", as(png) replace

	
	**  Construction Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line construction_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Construction GDP revisions") ///
	title("Construction GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/construction_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/construction_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/construction_revisions_m.png", as(png) replace

	
	**  Electricity Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line electricity_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Electricity GDP revisions") ///
	title("Electricity GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/electricity_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/electricity_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/electricity_revisions_m.png", as(png) replace


	**  Fishing Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line fishing_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Fishing GDP revisions") ///
	title("Fishing GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/fishing_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/fishing_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/fishing_revisions_m.png", as(png) replace
	
	
	**  Manufacturing Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line manufacturing_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Manufacturing GDP revisions") ///
	title("Manufacturing GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/manufacturing_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/manufacturing_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/manufacturing_revisions_m.png", as(png) replace	
	

	**  Mining Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line mining_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Mining GDP revisions") ///
	title("Mining GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/mining_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/mining_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/mining_revisions_m.png", as(png) replace		
	
	**  Services Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	
	** Plotting
	
	twoway (line services_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Services GDP revisions") ///
	title("Services GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export graph
	
	graph export "${graphs_folder}/services_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/services_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/services_revisions_m.png", as(png) replace
	
	
	
	
	