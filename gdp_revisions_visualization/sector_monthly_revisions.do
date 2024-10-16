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
		
	
	** Generate monthly var and change format from %td to %tm
	
	gen monthly = mofd(revision_date)
	format monthly %tm
	drop revision_date
	rename monthly revision_date // Return original var name
	
	
	** Order dataset
	
	order id revision_date
	
save temp_data, replace
	

	
/*----------------------
Computing revision mean
by sector 
-----------------------*/
	

use temp_data, clear
	
	** Definir un nuevo sufijo para las variables promedio
	
	local sufijo _mean
	
	
	** Iterar sobre cada variable en la lista
	
	foreach var of varlist *_revision {
		// Calcular el promedio de la variable actual y guardar en una nueva variable con el sufijo "_mean"
		egen `var'`sufijo' = mean(`var')
	}
	
save temp_data, replace

	
	
/*----------------------
Charts for Monthly GDP
Revisions by sector
-----------------------*/
	
	
use temp_data, clear	
	

	** Global GDP Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	"224 224 224" ///
	, n(3) nograph

	** Graph
	
	twoway (line gdp_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line gdp_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
		xtitle("", axis(1)) ///
		ytitle("GDP revisions") ///
		title("Global GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
		graphregion(color(white)) ///
		bgcolor(white) ///
		legend(position(1) label(1 "GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line agriculture_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line agriculture_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Agriculture GDP revisions") ///
	title("Agriculture GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Agriculture GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line commerce_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line commerce_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Commerce GDP revisions") ///
	title("Commerce GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Commerce GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line construction_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line construction_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Construction GDP revisions") ///
	title("Construction GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Construction GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line electricity_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line electricity_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Electricity GDP revisions") ///
	title("Electricity GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Electricity GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line fishing_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line fishing_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Fishing GDP revisions") ///
	title("Fishing GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Fishing GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line manufacturing_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line manufacturing_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Manufacturing GDP revisions") ///
	title("Manufacturing GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Manufacturing GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line mining_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line mining_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Mining GDP revisions") ///
	title("Mining GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Mining GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
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
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line services_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line services_revision_mean revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Services GDP revisions") ///
	title("Services GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "Services GDP revision") label(2 "Mean") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
	** Export graph
	
	graph export "${graphs_folder}/services_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/services_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/services_revisions_m.png", as(png) replace
	
	
	
/*----------------------
Charts for Monthly
Inter-sectoral GDP Revisions
-----------------------*/


	**  Global and Services GDP Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line gdp_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line services_revision revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("Revision date", axis(1)) ///
	ytitle("GDP revisions") ///
	title("Global and Services GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "GDP revision") label(2 "Services GDP revision") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
	** Export graph
	
	graph export "${graphs_folder}/gdp_services_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/gdp_services_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/gdp_services_revisions_m.png", as(png) replace
	
	
	**  Global and Mining GDP Monthly Revisions
	** ______________________________		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	"224 224 224" ///
	, n(3) nograph

	
	** Graph
	
	twoway (line gdp_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)) || ///
       (line mining_revision revision_date, lcolor("`r(p2)'%100") fintensity(*0.8)), ///
	xtitle("Revision date", axis(1)) ///
	ytitle("GDP revisions") ///
	title("Global and Mining GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	legend(position(1) label(1 "GDP revision") label(2 "Mining GDP revision") size(vsmall) order(1 2) ring(0) col(1) region(color("`r(p3)'%30")) title("Legend", size(*0.6)))
	
	
	** Export graph
	
	graph export "${graphs_folder}/gdp_mining_revisions_m.pdf", as(pdf) replace
	graph export "${graphs_folder}/gdp_mining_revisions_m.eps", as(eps) replace
	graph export "${graphs_folder}/gdp_mining_revisions_m.png", as(png) replace	