#*******************************************************************************
# Lines: Specific Annual Events by Horizon (1998 & 1999)
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: forecasts_events_by_horizon_a.R
# + First Created: 11/10/24
# + Last Updated: 12/15/24
#-------------------------------------------------------------------------------



#*******************************************************************************
# Libraries
#*******************************************************************************

# Load required packages for data processing and visualization
library(RPostgres)    # PostgreSQL database connection
library(ggplot2)      # Data visualization
library(lubridate)    # Date handling
library(svglite)      # SVG graphics export
library(dplyr)        # Data manipulation
library(tidyr)        # Data reshaping (pivot_longer)
library(tcltk)        # GUI elements for user input
library(sandwich)     # Robust standard errors
library(lmtest)       # Hypothesis testing
library(scales)       # Format number



#*******************************************************************************
# Initial Setup
#*******************************************************************************

# Use a dialog box to select the folder
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

# Define output directories
output_dir <- file.path(user_path, "charts")

# Create output directories if they do not exist
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("Directories created successfully in:", user_path, "\n")



#*******************************************************************************
# Database Connection
#*******************************************************************************

# Retrieve database credentials from environment variables
user <- Sys.getenv("CIUP_SQL_USER")
password <- Sys.getenv("CIUP_SQL_PASS")
host <- Sys.getenv("CIUP_SQL_HOST")
port <- 5432
database <- "gdp_revisions_datasets"

# Connect to PostgreSQL database
con <- dbConnect(RPostgres::Postgres(),
                 dbname = database,
                 host = host,
                 port = port,
                 user = user,
                 password = password)

# Fetch data for the selected sector
query <- "SELECT * FROM jefas_gdp_revisions_panel"
df <- dbGetQuery(con, query)

# Close database connection
dbDisconnect(con)



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Definir las fechas específicas que quieres graficar
selected_vintages <- as.Date(c("1998-12-01", "1999-01-01", "1999-02-01"))

# Filtrar los datos
df_filtered <- df %>%
  filter(vintages_date %in% selected_vintages)



#*******************************************************************************
# Visualization
#*******************************************************************************

ggplot(df_filtered, aes(x = horizon, y = gdp_release, color = as.factor(vintages_date))) +
  geom_line(size = 1) +
  geom_point(size = 2) +  # Agregar puntos para mayor visibilidad
  scale_color_manual(values = c("blue", "red", "green")) +  # Personaliza los colores
  labs(title = "Evolución de GDP Release por Horizon",
       x = "Horizon",
       y = "GDP Release",
       color = "Vintage Date") +
  theme_minimal()

