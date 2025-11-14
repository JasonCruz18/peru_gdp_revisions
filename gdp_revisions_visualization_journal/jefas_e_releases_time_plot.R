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
query <- "SELECT * FROM jefas_gdp_revisions_base_year"
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

# vintages_date aAs Date

# df$vintages_date <- dmy_hms(df$vintages_date) # for imported csv

# Verificar el resultado
head(df$vintages_date)

# Ensure that the 'vintages_date' column is of type Date
df$vintages_date <- as.Date(df$vintages_date)


df <- df %>%
  mutate(
    gdp_most_recent = ifelse(vintages_date >= as.Date("2020-03-01") & vintages_date <= as.Date("2021-10-01"), NaN, gdp_most_recent),
    gdp_release_1 = ifelse(vintages_date >= as.Date("2020-03-01") & vintages_date <= as.Date("2021-10-01"), NaN, gdp_release_1)
  )


#*******************************************************************************
# Suavizando
#*******************************************************************************

df <- df %>%
  mutate(
    gdp_most_recent_smooth = (lag(gdp_most_recent, 2) + 
                                2 * lag(gdp_most_recent, 1) + 
                                4 * gdp_most_recent + 
                                2 * lead(gdp_most_recent, 1) + 
                                lead(gdp_most_recent, 2)) / 10,
    
    gdp_release_1_smooth = (lag(gdp_release_1, 2) + 
                              2 * lag(gdp_release_1, 1) + 
                              4 * gdp_release_1 + 
                              2 * lead(gdp_release_1, 1) + 
                              lead(gdp_release_1, 2)) / 10
  )


# Filter the dataframe for the desired date range
df_filtered <- df[df$vintages_date >= as.Date("2001-01-01") & df$vintages_date <= as.Date("2023-10-31"), ]



#*******************************************************************************
# Plotting Function
#*******************************************************************************

# Define the date label format function
date_label_format <- function(date) {
  paste0(format(date, "%Y"))
}

# Generate a sequence of dates for the first month of each year
breaks_dates <- seq(from = as.Date("2001-01-01"), 
                    to = as.Date("2023-10-31"), 
                    by = "2 years")

# Create the graph
time_plot <- ggplot(df_filtered, aes(x = vintages_date)) +
  # Agregar regiones sombreadas con leyenda
  geom_rect(aes(xmin = as.Date("2013-01-01"), xmax = as.Date("2014-01-01"),
                ymin = -Inf, ymax = Inf, fill = "Cambio de año base"), alpha = 0.85) +
  geom_rect(aes(xmin = as.Date("2020-03-01"), xmax = as.Date("2021-10-01"),
                ymin = -Inf, ymax = Inf, fill = "COVID-19"), alpha = 0.85) +
  geom_line(aes(y = gdp_most_recent_smooth, color = "Publicación más reciente"), linewidth = 0.5) +
  geom_line(aes(y = gdp_release_1_smooth, color = "Publicación inicial"), linewidth = 0.85) +
  geom_bar(aes(y = e_1_gdp * 2.0, fill = "Error de predicción inicial"), 
           stat = "identity", alpha = 0.45, color = "black", linewidth = 0.35) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.45) +
  geom_point(aes(y = gdp_most_recent_smooth, color = "Publicación más reciente"), size = 0.85) +
  labs(
    x = NULL,
    y = "Publicaciones del PIB",
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
  scale_color_manual(values = c("Publicación inicial" = "#0079FF", 
                                "Publicación más reciente" = "#FF0060")) +  
  scale_fill_manual(values = c("Error de predicción inicial" = "#F5F5F5", 
                               "Cambio de año base" = "#00DFA2", 
                               "COVID-19" = "#F6FA70")) +  
  scale_y_continuous(
    breaks = scales::pretty_breaks(n = 5),
    labels = scales::number_format(accuracy = 0.1),
    sec.axis = sec_axis(~ . / 2.0, 
                        name = "Error de predicción inicial (escalado)", 
                        labels = scales::number_format(accuracy = 0.1))
  ) +
  scale_x_date(
    breaks = breaks_dates,
    labels = date_label_format,
    expand = c(0.02, 0.02)
  ) +
  coord_cartesian(ylim = c(-3.5, 11.5), clip = "off")  # Restrict Y-axis range

# Mostrar el gráfico
print(time_plot)


# Guardar el gráfico
plot_output_file <- file.path(output_dir, "e_releses_time_plot.png")
ggsave(filename = plot_output_file, plot = time_plot, width = 10, height = 6, dpi = 300, bg = "transparent")
