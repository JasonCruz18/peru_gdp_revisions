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
  par(bg = "transparent", mar = c(4, 5, 4, 2))  # Margen ajustado
  
  # Preparar fórmula y datos
  formula_str <- as.formula(paste0(sector, "_", variable, " ~ horizon"))
  bplt <- boxplot(
    formula = formula_str,
    data = data,
    plot = FALSE
  )
  
  # Calcular rangos y respiro lateral
  x_min <- min(bplt$group) - 0.35
  x_max <- max(bplt$group) + 0.35
  
  # Crear gráfico vacío con rejilla de fondo
  plot(1, type = "n", xlim = c(x_min, x_max), ylim = range(bplt$stats, na.rm = TRUE),
       xaxt = "n", yaxt = "n", xlab = "", ylab = "", bty = "n")
  
  # Rejilla horizontal (major Y)
  y_ticks <- pretty(range(bplt$stats, na.rm = TRUE))
  abline(h = y_ticks, col = "#F5F5F5", lwd = 1.6, lty = 1)
  
  # Rejilla vertical menor (minor X)
  x_minor_ticks <- seq(x_min, x_max, by = 0.5)
  abline(v = x_minor_ticks, col = "#F5F5F5", lwd = 1.4, lty = 1)
  
  # Dibujar boxplot encima de rejilla
  bplt <- boxplot(
    formula = formula_str,
    data = data,
    outline = FALSE,
    xlab = NULL,
    ylab = NULL,
    col = color,
    border = "black",
    lwd = 1.6,
    cex.axis = 1.5,
    cex.lab = 1.5,
    xaxt = "n",
    yaxt = "n",
    add = TRUE
  )
  
  # Eje X personalizado
  axis(1, at = seq_along(bplt$names), labels = bplt$names, cex.axis = 1.5)
  
  # Eje Y horizontal
  axis(2, cex.axis = 1.5, las = 1)
  
  # Agregar medias
  means <- sapply(levels(data$horizon), function(h) mean(data[data$horizon == h, paste0(sector, "_", variable)], na.rm = TRUE))
  points(seq_along(means), means, col = color, pch = 21, cex = 2.5, bg = "#292929", lwd = 2.0)
  
  # Leyenda
  legend(legend_position, legend = "Media", col = color, pch = 21, pt.cex = 2.5, cex = 1.5,
         pt.bg = "#292929", text.col = "#292929", horiz = TRUE, bty = "n", pt.lwd = 2.0)
  
  # Borde del gráfico
  box(lwd = 1.5)
  
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
  generate_boxplot(df_filtered_e, "e", "#F5F5F5", "bottomright", sector, output_dir)
}

cat("All plots have been generated successfully in:", output_dir, "\n")


