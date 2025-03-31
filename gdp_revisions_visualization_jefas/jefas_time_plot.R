#*******************************************************************************
# Time plot: 1st Revision, 1st Forecast Error and 12th Release
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: time_plot.R
# + First Created: 03/24/25
# + Last Updated: 03/24/25
#-------------------------------------------------------------------------------



#*******************************************************************************
# Libraries
#*******************************************************************************

# Load required packages
library(RPostgres)    # PostgreSQL database connection
library(dplyr)        # Data manipulation
library(ggplot2)      # Data visualization
library(tidyr)        # Data transformation
library(rstudioapi)   # User interaction (folder selection)
library(lubridate)


#*******************************************************************************
# Initial Setup
#*******************************************************************************

# Use a dialog box to select the folder
if (requireNamespace("rstudioapi", quietly = TRUE)) {
  # Check if rstudioapi is available for folder selection
  user_path <- rstudioapi::selectDirectory() # Open directory selection dialog
  if (!is.null(user_path)) {
    setwd(user_path)  # Set the working directory to the selected folder
    cat("The working directory has been set to:", getwd(), "\n")
  } else {
    cat("No folder was selected.\n")  # If no folder was selected
  }
} else {
  cat("Install the 'rstudioapi' package to use this functionality.\n")  # If rstudioapi is not installed
}

# Define output directories
output_dir <- file.path(user_path, "charts")  # Main output directory

# Create output directories if they do not exist
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("Directories created successfully in:", user_path, "\n")



#*******************************************************************************
# Database Connection
#*******************************************************************************

# Retrieve database credentials from environment variables
user <- Sys.getenv("CIUP_SQL_USER")  # PostgreSQL username
password <- Sys.getenv("CIUP_SQL_PASS")  # PostgreSQL password
host <- Sys.getenv("CIUP_SQL_HOST")  # Host of the PostgreSQL server
port <- 5432  # Default PostgreSQL port
database <- "gdp_revisions_datasets"  # Database name

# Connect to PostgreSQL database
con <- dbConnect(RPostgres::Postgres(),
                 dbname = database,
                 host = host,
                 port = port,
                 user = user,
                 password = password)

# Fetch data from the first table
query <- "SELECT * FROM jefas_gdp_revisions"
df <- dbGetQuery(con, query)

# Close the database connection
dbDisconnect(con)



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Format vintages_date as date column 

df$vintages_date <- dmy_hms(df$vintages_date)
df$vintages_date <- as.Date(df$vintages_date)  # Para eliminar la parte de hora

str(df$vintages_date)  # it should show "Date"
head(df$vintages_date)  # check the first dates

#unique(df$vintages_date)  # Ver los valores únicos

# Filter data to remove rows with missing values in key columns
df <- df %>% 
  filter(!is.na(vintages_date))

# Filter for vintages_dates > 30/11/1992 and vintages_dates < 01/11/2023
df <- df %>% 
  filter(vintages_date > as.Date("1992-12-31") & vintages_date < as.Date("2023-11-01"))



#*******************************************************************************
# Time Series Plot
#*******************************************************************************

time_plot <- ggplot(df, aes(x = vintages_date)) +
  geom_line(aes(y = gdp_most_recent, color = "12th release", group = 1), linewidth = 1.5) +
  geom_line(aes(y = e_1_gdp, color = "1st forecast error", group = 1), linewidth = 1.5) + 
  geom_line(aes(y = r_2_gdp, color = "1st revision", group = 1), linewidth = 1.5) +
  scale_color_manual(values = c("12th release" = "#292929", 
                                "1st forecast error" = "#FF0060", 
                                "1st revision" = "#0079FF")) +
  scale_x_date(breaks = seq(as.Date("1995-01-01"), 
                            max(df$vintages_date, na.rm = TRUE), 
                            by = "5 years"), 
               date_labels = "%Y") +  # Mostrar solo el año en el eje x
  labs(title = "12th Release, 1st Forecast Error and 1st Revision by Target Events",
       x = "Horizon",
       y = "GDP Revisions",
       color = "") +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 20, face = "bold"),  # Tamaño del título principal
    legend.position = "bottom",  # Ubicar la leyenda debajo del gráfico
    legend.box = "horizontal",
    legend.text = element_text(size = 14),  # Tamaño de fuente de la leyenda
    legend.title = element_text(size = 14), # Tamaño de fuente del título de la leyenda
    legend.background = element_rect(color = "#292929", fill = "white", linewidth = 1.0), # Borde de la leyenda
    panel.grid.major = element_line(color = "#F5F5F5", linewidth = 1.2),  # Grillas principales
    panel.grid.minor = element_line(color = "#F5F5F5", linewidth = 1.2),  # Grillas secundarias
    panel.border = element_rect(color = "#292929", fill = NA, linewidth = 1.0),  # Contorno completo del gráfico
    axis.title = element_text(size = 16),  # Tamaño de fuente de los títulos de los ejes
    axis.text = element_text(size = 14)  # Tamaño de fuente de los valores de los ejes
  ) +
  guides(color = guide_legend(nrow = 1))  # Poner la leyenda en una sola fila

time_plot


# Definir el archivo de salida

output_file <- file.path(output_dir, "time_plot.png")

# Guardar el gráfico en alta resolución

ggsave(filename = output_file, plot = time_plot, width = 10, height = 6, dpi = 300, bg = "transparent")
