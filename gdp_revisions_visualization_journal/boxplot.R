#*******************************************************************************
# Boxplots: Forecast Errors by Horizon by Pooling Fixed-Event Forecasts
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: boxplot_m.R
# + First Created: 11/10/24
# + Last Updated: 12/15/24
#-------------------------------------------------------------------------------

#*******************************************************************************
# Libraries
#*******************************************************************************

library(RPostgres)
library(dplyr)
library(tidyr)
library(scales)
library(tcltk)

#*******************************************************************************
# Initial Setup
#*******************************************************************************

if (requireNamespace("rstudioapi", quietly = TRUE)) {
  user_path <- rstudioapi::selectDirectory()
  if (!is.null(user_path)) {
    setwd(user_path)
    cat("The working directory has been set to:", getwd(), "\n")
  } else {
    cat("No folder was selected.\n")
  }
} else {
  cat("Install the 'rstudioapi' package to use this functionality.\n")
}

output_dir <- file.path(user_path, "charts")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
cat("Directories created successfully in:", user_path, "\n")

#*******************************************************************************
# Database Connection
#*******************************************************************************


#-------------------------------------------------------------------------------
# Import from PostgresSQL
#-------------------------------------------------------------------------------

#user <- Sys.getenv("CIUP_SQL_USER")
#password <- Sys.getenv("CIUP_SQL_PASS")
#host <- Sys.getenv("CIUP_SQL_HOST")
#port <- 5432
#database <- "gdp_revisions_datasets"

#con <- dbConnect(RPostgres::Postgres(), dbname = database, host = host, port = port, user = user, password = password)

#df <- dbGetQuery(con, "SELECT * FROM jefas_gdp_revisions_panel")

#dbDisconnect(con)


#-------------------------------------------------------------------------------
# Import from Excel or csv
#-------------------------------------------------------------------------------

# Load readxl package (install if needed)
if (!requireNamespace("readxl", quietly = TRUE)) {
  install.packages("readxl")
}
library(readxl)

# Import Excel file into R
r_gdp_revisions_panel <- read_excel(
  path = file.path(user_path, "r_gdp_revisions_panel.xlsx")
)

# Quick check
cat("Data imported successfully. Dimensions:", 
    nrow(r_gdp_revisions_panel), "rows and", 
    ncol(r_gdp_revisions_panel), "columns.\n")



#*******************************************************************************
# Data Preparation
#*******************************************************************************

df <- r_gdp_revisions_panel %>% 
  filter(!is.na(horizon) & !is.na(target_period) & horizon >= 1 & horizon < 11)

df$horizon <- factor(df$horizon, levels = as.character(1:11))



#*******************************************************************************
# Plotting Function
#*******************************************************************************
    
generate_boxplot <- function(data, variable, color, legend_position, output_dir) {
  output_file <- file.path(output_dir, paste0(variable, "_boxplot_m", ".png"))
  png(filename = output_file, width = 16, height = 9, units = "in", res = 300)  # +1 inch vertical para leyenda
  
  # Set plot margins and background
  par(bg = "white", mar = c(2.25, 3.75, 0.5, 0.5))  # Adjusted margins
  
  # Ensure 'horizon' is treated as a factor
  data$horizon <- factor(data$horizon, levels = 1:12)
  
  # Prepare the formula for plotting
  formula_str <- as.formula(paste0(variable, " ~ horizon"))
  
  # Create the boxplot without plotting (to gather stats)
  bplt <- boxplot(
    formula = formula_str,
    data = data,
    plot = FALSE
  )
  
  # Calculate the min and max for the X axis (horizon)
  x_min <- min(bplt$group) - 0.5
  x_max <- max(bplt$group) + 0.5
  
  # Create an empty plot with Y-axis limits from -1.0 to 1.5
  plot(1, type = "n", xlim = c(x_min, x_max), ylim = c(-1.0, 1.5),
       xaxt = "n", yaxt = "n", xlab = "", ylab = "", bty = "n")
  
  # Add horizontal grid lines for every 0.25 from -0.75 to 1.25 (minor grid)
  y_minor_ticks <- seq(-0.75, 1.25, by = 0.25)
  abline(h = y_minor_ticks, col = "#F5F5F5", lwd = 1.65, lty = 1)
  
  # Add major grid lines at -0.5, 0.0, 0.5, 1.0
  y_major_ticks <- c(-0.5, 0.0, 0.5, 1.0)
  abline(h = y_major_ticks, col = "#F5F5F5", lwd = 2, lty = 1)
  
  # Add vertical grid lines (minor grid lines)
  x_minor_ticks <- seq(x_min, x_max, by = 0.5)
  abline(v = x_minor_ticks, col = "#F5F5F5", lwd = 1.65, lty = 1)
  
  # Draw the boxplot with transparency
  col_alpha <- adjustcolor(color, alpha.f = 0.75)
  bplt <- boxplot(
    formula = formula_str,
    data = data,
    outline = FALSE,
    xlab = NULL,
    ylab = NULL,
    col = col_alpha,
    border = "#292929",
    lwd = 2.70,
    cex.axis = 1.75,
    cex.lab = 1.75,
    xaxt = "n",
    yaxt = "n",
    add = TRUE
  )
  
  # Add X-axis labels (horizon values)
  axis(1, at = seq_along(bplt$names), labels = bplt$names, cex.axis = 1.70)
  
  # Add Y-axis labels with ticks from -1.0 to 1.5 at 0.5 intervals
  axis(2, at = y_major_ticks, labels = y_major_ticks, cex.axis = 1.70, las = 1)
  
  # Calculate and add mean points
  means <- tapply(data[[variable]], data$horizon, mean, na.rm = TRUE)  # Calculate mean for each horizon
  
  # Plot the mean points in the middle of the boxes
  points(seq_along(means), means, col = color, pch = 21, cex = 3.35, bg = "#292929", lwd = 2.0)
  
  # Add the box around the plot
  box(lwd = 1.5)
  
  # Add the legend inside the plot, top-right
  legend("topright", inset = c(0.016, 0.03), legend = "Average", col = color,
         pch = 21, pt.cex = 3.15, cex = 1.55, pt.bg = "#292929",
         text.col = "black", horiz = TRUE, bty = "o", pt.lwd = 2.0,
         box.lwd = 1.5, xpd = TRUE, x.intersp = 0.6, y.intersp = 0.4)
  
  dev.off()
}




#*******************************************************************************
# Generate Plot for e
#*******************************************************************************

df_filtered_e <- df %>% 
  filter(!is.na(.data$e))  # Mantiene horizon = 1 en e

cat("Generating plots for variable e\n")
generate_boxplot(df_filtered_e, "e", "#F5F5F5", "topright", output_dir)

cat("All plots have been generated successfully in:", output_dir, "\n")
            

