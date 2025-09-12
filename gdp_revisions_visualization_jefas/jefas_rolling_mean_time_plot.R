#*******************************************************************************
# Lines and Bars: First, Most Recent Forecast and Final Revision   
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: e_releases_time_plot.R
# + First Created: 03/28/25
# + Last Updated: 03/29/25
#-------------------------------------------------------------------------------



#*******************************************************************************
# Libraries
#*******************************************************************************

# Load required packages
library(RPostgres)    # For connecting to PostgreSQL databases
library(ggplot2)      # For data visualization
library(lubridate)    # For date handling and manipulation
library(svglite)      # For creating SVG graphics
library(dplyr)        # For data manipulation and transformation
library(tidyr)        # For reshaping data
library(scales)       # For formatting numbers



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
query <- "SELECT * FROM jefas_rolling_stats_affected"
df <- dbGetQuery(con, query)

# Close the database connection
dbDisconnect(con)



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Define the sectors to iterate over for plotting
#sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing", 
#             "electricity", "construction", "commerce", "services")
sectors <- c("gdp")

# start aAs Date

# df$start <- dmy_hms(df$start) # for imported csv

# Verificar el resultado
head(df$start)

# Ensure that the 'start' column is of type Date
df$start <- as.Date(df$start)



#*******************************************************************************
# Suavizando
#*******************************************************************************

# Filter the dataframe for the desired date range
df_filtered <- df[df$start >= as.Date("1993-01-01") & df$start <= as.Date("2023-10-31"), ]



#*******************************************************************************
# Plotting Function
#*******************************************************************************

# Define the date label format function
date_label_format <- function(date) {
  paste0(format(date, "%Y"))
}

# Generate a sequence of dates for the first month of each year
breaks_dates <- seq(from = as.Date("1993-01-01"), 
                    to = as.Date("2017-10-31"), 
                    by = "2 years")

# Create the graph
time_plot <- ggplot(df_filtered, aes(x = start)) +
  # Agregar líneas principales
  geom_line(aes(y = mean_e1, color = "Rolling mean"), linewidth = 1.25) +
  geom_line(aes(y = mean_e1_affected, color = "Rolling mean (base year)"),
            linewidth = 0.65, alpha = 0.65, linetype = "dashed") +
  
  # Línea vertical en 2001-01-01
  geom_vline(xintercept = as.Date("2001-01-01"), 
             color = "#F5F5F5", 
             #linetype = "dashed", 
             linewidth = 3, 
             alpha = 0.85) +
  geom_vline(xintercept = as.Date("2001-01-01"), 
             color = "black", 
             linetype = "dashed", 
             linewidth = 0.3, 
             alpha = 1) +
  
  labs(
    x = NULL,
    y = NULL,
    title = NULL,
    color = NULL,
    fill = NULL
  ) +
  theme_minimal() +
  theme(
    panel.grid.major = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 16),
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = 16, color = "black"),
    plot.title = element_blank(),
    legend.position = "bottom",
    legend.title = element_blank(),
    legend.text = element_text(size = 16, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.45),
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),
    plot.margin = margin(9, 5, 9, 4)
  ) +
  scale_color_manual(values = c("Rolling mean" = "#3366FF", 
                                "Rolling mean (base year)" = "#3366FF")) +
  scale_x_date(
    breaks = seq(as.Date("1993-01-01"), as.Date("2017-01-01"), by = "2 years"),
    date_labels = "%Y",
    limits = c(as.Date("1993-01-01"), as.Date("2017-01-01")),
    expand = c(0.02, 0.02)
  )

# Mostrar el gráfico
print(time_plot)



# Guardar el gráfico
plot_output_file <- file.path(output_dir, "rolling_mean.png")
ggsave(filename = plot_output_file, plot = time_plot, width = 10, height = 6, dpi = 300, bg = "white")

