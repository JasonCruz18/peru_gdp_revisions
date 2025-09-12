#*******************************************************************************
# Lines: Stats of GDP Revisions by Horizon (Pooled Events)
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: r_e_quantiles.R
# + First Created: 03/22/25
# + Last Updated: 03/22/25
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
query_1 <- "SELECT * FROM r_sectorial_gdp_monthly_panel"
df_1 <- dbGetQuery(con, query_1)

# Fetch data from the second table
query_2 <- "SELECT * FROM e_sectorial_gdp_monthly_panel"
df_2 <- dbGetQuery(con, query_2)

# Close the database connection
dbDisconnect(con)



#*******************************************************************************
# Data Merging
#*******************************************************************************

# Merge the two datasets loaded from PostgreSQL using a full join
merged_df <- df_1 %>%
  full_join(df_2, by = c("vintages_date", "horizon"))  # Replace with actual common column names

# Sort merged_df by "vintages_date" and "horizon"
merged_df <- merged_df %>%
  arrange(vintages_date, horizon)

cat("Datasets merged successfully. Rows in merged data frame:", nrow(merged_df), "\n")


#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Filter data to remove rows with missing values in key columns
merged_df <- merged_df %>% 
  filter(!is.na(horizon) & !is.na(vintages_date))

# Filter for vintages_dates > 30/11/1992 and vintages_dates < 01/11/2023
merged_df <- merged_df %>% 
  filter(vintages_date > as.Date("1992-12-31") & vintages_date < as.Date("2023-11-01"))

# Filter data by horizon values (>1 & <13) for relevant analysis on r and e
r_merged_df <- merged_df %>% filter(horizon > 1 & horizon < 13)
e_merged_df <- merged_df %>% filter(horizon > 0 & horizon < 12)

# Convert 'horizon' to a factor for categorical analysis in the plots
r_merged_df$horizon <- as.factor(r_merged_df$horizon)
e_merged_df$horizon <- as.factor(e_merged_df$horizon)



#*******************************************************************************
# Summary Statistics
#*******************************************************************************

# Revisions
#------------------------

r_summary_data <- r_merged_df %>%
  group_by(horizon) %>%
  summarise(
    p1 = quantile(r_gdp, probs = 0.01, na.rm = TRUE),
    median_r_gdp = median(r_gdp, na.rm = TRUE),
    p99 = quantile(r_gdp, probs = 0.99, na.rm = TRUE)
  ) %>%
  ungroup() # Asegura que no haya problemas con el agrupamiento en ggplot


# Forecast errors
#------------------------

e_summary_data <- e_merged_df %>%
  group_by(horizon) %>%
  summarise(
    p1 = quantile(e_gdp, probs = 0.01, na.rm = TRUE),
    median_r_gdp = median(e_gdp, na.rm = TRUE),
    p99 = quantile(e_gdp, probs = 0.99, na.rm = TRUE)
  ) %>%
  ungroup() # Asegura que no haya problemas con el agrupamiento en ggplot



#*******************************************************************************
# Time Series Plot
#*******************************************************************************

# Revisions
#------------------------

r_gdp_plot <- ggplot(r_summary_data, aes(x = horizon)) +
  geom_line(aes(y = p1, color = "P1 (1st percentile)", group = 1), linewidth = 1.5) +
  geom_point(aes(y = median_r_gdp, color = "Median"), size = 3.5, shape = 21, fill = scales::alpha("#F5F5F5", 0.5), stroke = 1.5) + 
  geom_line(aes(y = p99, color = "P99 (99th percentile)", group = 1), linewidth = 1.5) +
  scale_color_manual(values = c("P1 (1st percentile)" = "#FF0060", 
                                "Median" = "#292929", 
                                "P99 (99th percentile)" = "#0079FF")) +
  labs(title = "Peruvian GDP Revisions Percentiles and Median",
       x = "Horizon",
       y = "GDP Revisions",
       color = "Statistic") +
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

r_gdp_plot


# Forecast errors
#------------------------

e_gdp_plot <- ggplot(e_summary_data, aes(x = horizon)) +
  geom_line(aes(y = p1, color = "P1 (1st percentile)", group = 1), linewidth = 1.5) +
  geom_point(aes(y = median_r_gdp, color = "Median"), size = 3.5, shape = 21, fill = scales::alpha("#F5F5F5", 0.5), stroke = 1.5) + 
  geom_line(aes(y = p99, color = "P99 (99th percentile)", group = 1), linewidth = 1.5) +
  scale_color_manual(values = c("P1 (1st percentile)" = "#FF0060", 
                                "Median" = "#292929", 
                                "P99 (99th percentile)" = "#0079FF")) +
  labs(title = "Peruvian GDP Forecast Errors Percentiles and Median",
       x = "Horizon",
       y = "GDP Revisions",
       color = "Statistic") +
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

e_gdp_plot

# Definir el archivo de salida
r_output_file <- file.path(output_dir, "gdp_r_quantiles.png")
e_output_file <- file.path(output_dir, "gdp_e_quantiles.png")

# Guardar el gráfico en alta resolución
ggsave(filename = r_output_file, plot = r_gdp_plot, width = 10, height = 6, dpi = 300, bg = "transparent")
ggsave(filename = e_output_file, plot = e_gdp_plot, width = 10, height = 6, dpi = 300, bg = "transparent")
