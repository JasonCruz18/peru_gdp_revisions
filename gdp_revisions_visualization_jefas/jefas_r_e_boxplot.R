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
library(ggplot2)
library(lubridate)
library(svglite)
library(dplyr)
library(tidyr)
library(sandwich)
library(lmtest)
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

user <- Sys.getenv("CIUP_SQL_USER")
password <- Sys.getenv("CIUP_SQL_PASS")
host <- Sys.getenv("CIUP_SQL_HOST")
port <- 5432
database <- "gdp_revisions_datasets"

con <- dbConnect(RPostgres::Postgres(), dbname = database, host = host, port = port, user = user, password = password)

df <- dbGetQuery(con, "SELECT * FROM jefas_gdp_revisions_panel")

dbDisconnect(con)

#*******************************************************************************
# Data Preparation
#*******************************************************************************

sectors <- c("gdp")

merged_df <- df %>% 
  filter(!is.na(horizon) & !is.na(vintages_date) & horizon >= 1 & horizon < 12)

merged_df <- merged_df %>%
  filter(vintages_date > as.Date("2000-12-31") & vintages_date < as.Date("2023-11-01"))

merged_df$horizon <- factor(merged_df$horizon, levels = as.character(1:11))

#*******************************************************************************
# Plotting Function
#*******************************************************************************

generate_boxplot <- function(data, variable, color, legend_position, sector, output_dir) {
  output_file <- file.path(output_dir, paste0(variable, "_boxplot_", sector, "_m", ".png"))
  png(filename = output_file, width = 10, height = 6, units = "in", res = 300)
  par(bg = "transparent", mar = c(2.0, 2.55, 1.2, 0.2))
  
  boxplot(
    formula = as.formula(paste0(sector, "_", variable, " ~ horizon")), 
    data = data, 
    outline = FALSE,
    xlab = NA,
    ylab = NA,
    col = color,
    border = "#292929",
    lwd = 3.0,
    cex.axis = 2.2,
    cex.lab = 2.2,
    xaxt = "n",
    yaxt = "n"
  )
  
  # Ajuste de etiquetas de eje X desde 1
  axis(1, at = seq_along(levels(data$horizon)), labels = levels(data$horizon), cex.axis = 2.2)
  
  y_ticks <- axTicks(2)
  axis(1, at = seq_along(levels(data$horizon)), labels = levels(data$horizon), cex.axis = 2.2)
  box(lwd = 2.5)
  
  # Calcular y agregar medias
  means <- sapply(levels(data$horizon), function(h) mean(data[data$horizon == h, paste0(sector, "_", variable)], na.rm = TRUE))
  points(seq_along(means), means, col = color, pch = 21, cex = 3.5, bg = "black", lwd = 2.0)
  
  # Agregar leyenda
  legend(legend_position, legend = "Media", col = color, pch = 21, pt.cex = 3.5, cex = 2.5,
         pt.bg = "black", text.col = "black", horiz = TRUE, bty = "n", pt.lwd = 2.0)
  
  dev.off()
}

#*******************************************************************************
# Generate Plots for e and r for All Sectors
#*******************************************************************************

for (sector in sectors) {
  df_filtered_e <- merged_df %>% 
    filter(!is.na(.data[[paste0(sector, "_e")]]))  # Mantiene horizon = 1 en gdp_e
  
  df_filtered_r <- merged_df %>% 
    filter(!is.na(.data[[paste0(sector, "_r")]]))  # Puede no incluir horizon = 1
  
  cat("Generating plots for sector:", sector, "\n")
  
  generate_boxplot(df_filtered_r, "r", "#0079FF", "bottomleft", sector, output_dir)
  generate_boxplot(df_filtered_e, "e", "#FF0060", "bottomright", sector, output_dir)
}

cat("All plots have been generated successfully in:", output_dir, "\n")

