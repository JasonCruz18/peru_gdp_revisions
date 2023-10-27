/********************
Stats for legal particular data
***

	Author
	---------------------
	Jason Cruz
	*********************/

	*** Program: stats_legal_data
	** 	First Created: 26/03/22
	** 	Last Updated:  26/03/22

		
		
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

	di `"Please, input the path for storing the outputs of this dofile into the COMMAND WINDOW and then press ENTER  "'  _request(path)
	cd "$path"


/*----------------------
Import ODBC dataset and
save temp
-----------------------*/


import excel "documents_law", firstrow // In case you use your local path, make sure you have the "documents_law" file
		
//odbc load, exec("select * from claims_21_dataset_algoritmo") dsn("algorithm_data") lowercase sqlshow clear // When we use ODBC server

save TempData, replace
	
	
	
/*----------------------
Parsing dataset
-----------------------*/
	
	
use TempData, clear

	d // check entire dataset variables
	
	
	** Transform all variable names to lower case
	
	rename _all, lower
	
	
	** Vars label as vars names (lowercase)
	
	foreach var of varlist _all {
    label variable `var' "`var'"
	}
	

	** Checking null values and duplicates for a claim
	
	duplicates list documents // There are duplicates
	
	sort documents
	quietly by documents:  gen dup = cond(_N==1,0,_n)
	tab dup 	
	
	duplicates drop documents, force
	
	duplicates list documents // (0 observations are duplicates)

	misstable summarize // checking null values (variables nonmissing or string)
	
	drop dup
	
	
	** Drop stop var and generate index var
	
	drop a
	gen index = _n
	
	** Checking row number
	
	count // 7,356 parsed observations

save "law_data_21", replace


	
/*----------------------
Reporting statistics for
law particular data
-----------------------*/


use "law_data_21", clear


	** Generate dummy vars
	
	gen resolucion_yn = 0
	replace resolucion_yn = 1 if documents_resolucion != "[]" & ///
	documents_decreto == "[]" & ///
	documents_oficio == "[]" & ///
	documents_directiva == "[]" & ///
	documents_otros == "[]"
	
	gen decreto_yn = 0
	replace decreto_yn = 1 if documents_decreto != "[]" & ///
	documents_resolucion == "[]" & ///
	documents_oficio == "[]" & ///
	documents_directiva == "[]" & ///
	documents_otros == "[]"
	
	gen oficio_yn = 0
	replace oficio_yn = 1 if documents_oficio != "[]" & ///
	documents_resolucion == "[]" & ///
	documents_decreto == "[]" & ///
	documents_directiva == "[]" & ///
	documents_otros == "[]"
	
	gen directiva_yn = 0
	replace directiva_yn = 1 if documents_directiva != "[]" & ///
	documents_resolucion == "[]" & ///
	documents_oficio == "[]" & ///
	documents_decreto == "[]" & ///
	documents_otros == "[]"
	
	gen otros_yn = 0
	replace otros_yn = 1 if documents_otros != "[]" & ///
	documents_resolucion == "[]" & ///
	documents_oficio == "[]" & ///
	documents_directiva == "[]" & ///
	documents_decreto == "[]"
	
	
	// otros contain: ley, articulo, informe, circular, marco & marco directiva
	
	
	** Generate dummy vars at least
	
	gen resolucion_al = 0
	replace resolucion_al = 1 if documents_resolucion != "[]"
	
	gen decreto_al = 0
	replace decreto_al = 1 if documents_decreto != "[]"
	
	gen oficio_al = 0
	replace oficio_al = 1 if documents_oficio != "[]"
	
	gen directiva_al = 0
	replace directiva_al = 1 if documents_directiva != "[]"
	
	gen otros_al = 0
	replace otros_al = 1 if documents_otros != "[]"
	
	// otros contain: ley, articulo, informe, circular, marco & marco directiva
	
	** Generate dummy vars
	
	gen law_yn = 0
	replace law_yn = 1 if resolucion_yn == 1 | ///
	decreto_yn == 1 | ///
	oficio_yn == 1 | ///
	directiva_yn == 1 | ///
	otros_yn == 1
	
	
	** Generate dummy vars
	
	gen law_al = 0
	replace law_al = 1 if resolucion_al == 1 | ///
	decreto_al == 1 | ///
	oficio_al == 1 | ///
	directiva_al == 1 | ///
	otros_al == 1
	
	** Generate total row number variable
	
	by index, sort: gen amount_docs = _n == 1
	by index: replace amount_docs  = sum(amount_docs)
	by index: replace amount_docs  = amount_docs[_N]
	
	
	** 	Collapse interesting vars
	
	collapse (sum) *_yn *_al amount_docs
	
	
	** Generate percentage vars
	
	gen per_law_yn = (law_yn/amount_docs) *100
	gen per_complement_yn = 100-per_law
	
	gen per_resolucion_yn = (resolucion_yn/law_yn) *100
	gen per_decreto_yn = (decreto_yn/law_yn) *100
	gen per_oficio_yn = (oficio_yn/law_yn) *100
	gen per_directiva_yn = (directiva_yn/law_yn) *100
	gen per_otros_yn = (otros_yn/law_yn) *100
	
	
	** Generate percentage vars
	
	gen per_law_al = (law_al/amount_docs) *100
	gen per_complement_al = 100-per_law_al
	gen total_law_al = per_law_al + per_complement_al
	
	gen per_resolucion_al = (resolucion_al/law_al) *100
	gen per_decreto_al = (decreto_al/law_al) *100
	gen per_oficio_al = (oficio_al/law_al) *100
	gen per_directiva_al = (directiva_al/law_al) *100
	gen per_otros_al = (otros_al/law_al) *100
	
	
	
/*----------------------
Ploting stats for law
particular data
yn: (yes or not)
-----------------------*/
	
	
	** First graph
	** -----------
	
	** Set up the color palette
	
	colorpalette ///
	"16 86 79" ///
	"19 171 97" ///
	"0 125 90" ///
	"25 57 65" ///
	"0 180 140" ///
	, n(5) nograph
	
	
	** Set up graph options
		
	graph pie per_law_yn  per_complement_yn, ///
	plabel(1 "20,9 %", color(white) size(*1.5)) plabel(2 "79,1 %", color(white) size(*1.5)) ///
	line(lcolor(white) lwidth(medium) lalign(center)) ///
	pie(1, color("`r(p1)'")) pie(2, color("`r(p2)'")) ///
	legend( title("(%)", size(*0.55) position(1)) cols(1) label(1 "Law Data") label(2 "No Law Data") size(*0.5) ring(0) position(2) bmargin(zero) color(gs1) order(1 2) region(col(white)) /*placement(s)*/ rowgap(0) colgap(*0.8)) ///
	title("Proportion of observations related to a single type of legal data", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export plot to PDF, EPS and PNG format
	
	graph export "pie_law_data_all_yn.pdf", as(pdf) replace
	graph export "pie_law_data_all_yn.eps", as(eps) replace
	graph export "pie_law_data_all_yn.png", as(png) replace
	
	
	** Second graph
	** -----------
	
	
	** Set up the color palette
	
	colorpalette ///
	"16 86 79" ///
	"19 171 97" ///
	"0 125 90" ///
	"25 57 65" ///
	"0 180 140" ///
	, n(5) nograph
	
	
	** Set up graph options
		
	graph pie per_resolucion_yn per_decreto_yn per_oficio_yn per_directiva_yn per_otros_yn, ///
	plabel(1 "22,1 %", color(white) size(*1.5)) plabel(2 "6,8 %", color(white) size(*1.5)) plabel(3 "44,2 %", color(white) size(*1.5)) plabel(4 "4,7 %", color(white) size(*1.5)) plabel(5 "22,1 %", color(white) size(*1.5)) ///
	line(lcolor(white) lwidth(medium) lalign(center)) ///
	pie(1, color("`r(p1)'")) pie(2, color("`r(p2)'")) ///
	pie(3, color("`r(p3)'")) pie(4, color("`r(p4)'")) pie(5, color("`r(p5)'")) ///
	legend( title("(%)", size(*0.55) position(1)) cols(1) label(1 "Resolución") label(2 "Decreto") label(3 "Oficio") label(4 "Directiva") label(5 "Otros") size(*0.5) ring(0) position(2) bmargin(zero) color(gs1) order(1 2 3 4 5) region(col(white)) /*placement(s)*/ rowgap(0) colgap(*0.8)) ///
	title("Distribution of the proportions regarding legal data types", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	note("Observations that have only one type of legal data", size(*0.65))
	
	
	** Export plot to PDF, EPS and PNG format
	
	graph export "pie_law_data_types_yn.pdf", as(pdf) replace
	graph export "pie_law_data_types_yn.eps", as(eps) replace
	graph export "pie_law_data_types_yn.png", as(png) replace
	

	
/*----------------------
Ploting stats for law
particular data
al: at least one type
of law data
-----------------------*/


	** First graph
	** -----------
	
	
	** Gen string label var
	
	gen tolabel = string(per_law_al, "%2.0f") + "%"
	
	
	** Set up the color palette
	
	colorpalette ///
	"16 86 79" ///
	"19 171 97" ///
	"0 125 90" ///
	"25 57 65" ///
	"0 180 140" ///
	, n(5) nograph
	
	
	** Set up graph options
		
	twoway (bar total_law_al per_complement_al, bcolor("`r(p3)'%45") fintensity(*0.8)) ///
	(bar per_law_al per_complement_al, bcolor("`r(p3)'")) ///
	(scatter per_law_al per_complement_al, ms(none) mlabel(tolabel) mlabcolor(white) mlabsize(huge) mlabposition(6)), ///
	xscale(lstyle(none)) ///
	yscale(lstyle(none)) ///
	xtitle("") ///
	ytitle("") ///
	xlabel(, noticks nolabels) ///
	ylabel(, noticks nolabels) ///
	legend( title("(%)", size(*0.55) position(1)) cols(1) label(1 "No Legal Data") label(2 "At Least One Type") size(*0.5) ring(0) position(2) bmargin(zero) color(gs1) order(1 2) region(col(white)) /*placement(s)*/ rowgap(0) colgap(*0.8)) ///
	title("Proportion of observations relating to at least one type of legal data", size(*0.55) box bexpand bcolor("`r(p4)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	** Export plot to PDF, EPS and PNG format
	
	graph export "bar_law_data_all_al.pdf", as(pdf) replace
	graph export "bar_law_data_all_al.eps", as(eps) replace
	graph export "bar_law_data_all_al.png", as(png) replace
	
	
	** Second graph
	** -----------
	
	
	** Generate some useful vars
	
	keep per_*_al
	
	drop per_law_al per_complement_al
	
	gen id = _n 
	
	reshape long per_, i(id) j(law_types) string
	
	rename per_ per_types
	
	gen complement_law = 100-per_types
	
	gen total_law = 100
	
	replace id = _n
	
	gen tolabel_types = string(per_types, "%2.0f") + "%"
	
	
	** Set up the color palette
	
	colorpalette ///
	"16 86 79" ///
	"19 171 97" ///
	"0 125 90" ///
	"25 57 65" ///
	"0 180 140" ///
	, n(5) nograph
	
	
	** Set up graph options
			
	twoway (bar total_law id, barwidth(0.5) bcolor("`r(p3)'%45") fintensity(*0.8)) ///
	(bar per_types id, barwidth(0.5) bcolor("`r(p3)'")) ///
	(scatter per_types id, ms(none) mlabel(tolabel_types) mlabcolor(white) mlabsize(medium) mlabposition(6)), ///
	xscale(lstyle(none)) ///
	yscale(lstyle(none)) ///
	xtitle("") ///
	ytitle("") ///
	xlabel(1 "Decreto" 2 "Directiva" 3 "Oficio" 4 "Otros" 5 "Resolución", noticks ) ///
	ylabel(, noticks nolabels) ///
	legend(cols(1) label(1 "No Legal Data") label(2 "At Least This Type") size(*0.5) /*ring(1)*/ position(3) /*bmargin(zero)*/ color(gs1) order(1 2) region(col(white)) rowgap(2) colgap(*0.2) stack) ///
	title("Distribution of the proportions regarding legal data types", size(*0.55) box bexpand bcolor("`r(p4)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white) ///
	note("Observations that have at least one type of legal data", size(*0.65) position(6))
		
	
	** Export plot to PDF, EPS and PNG format
	
	graph export "bar_law_data_types_al.pdf", as(pdf) replace
	graph export "bar_law_data_types_al.eps", as(eps) replace
	graph export "bar_law_data_types_al.png", as(png) replace
