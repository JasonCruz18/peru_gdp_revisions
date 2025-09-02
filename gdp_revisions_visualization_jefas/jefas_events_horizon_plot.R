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
library(zoo)   # for as.yearmon

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
# Import from CSV file
#*******************************************************************************

# Define file path
file_path <- "C:/Users/Jason Cruz/OneDrive/Documentos/jefas_nowcasting_panel.csv"

# Read CSV into dataframe
df <- read.csv(file_path)

# If you prefer readr (tidyverse) for faster import:
# library(readr)
# df <- read_csv(file_path)



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Asegurar que target_period es de tipo Date
df <- df %>%
  mutate(target_period = as.Date(target_period))  # Convertir a Date si es necesario

# Definir las fechas específicas que quieres graficar
selected_vintages <- as.Date(c("2022-07-01", "2022-08-01", "2022-09-01"))

# Filtrar los datos
df_filtered <- df %>%
  filter(target_period %in% selected_vintages)

# Crear etiquetas amigables para la leyenda
df_filtered <- df_filtered %>%
  mutate(vintage_label = case_when(
    target_period == as.Date("2022-07-01") ~ "2022m07",
    target_period == as.Date("2022-08-01") ~ "2022m08",
    target_period == as.Date("2022-09-01") ~ "2022m09",
    TRUE ~ as.character(target_period)  # En caso de otros valores no especificados
  ))

# Definir formas específicas para cada línea
shape_values <- c("2022m07" = 16,  # Círculo (point)
                  "2022m08" = 15,   # Cuadrado (square)
                  "2022m09" = 17)   # Triángulo (triangle)



#*******************************************************************************
# Visualization
#*******************************************************************************

# Graficar con símbolos diferentes
horizon_plot <- ggplot(df_filtered, aes(x = horizon, y = gdp_release, color = vintage_label, shape = vintage_label)) +
  geom_line(linewidth = 1.2) +  # Grosor de línea
  geom_point(size = 4.0) +  # Tamaño de los puntos con forma específica
  #geom_hline(yintercept = 0, color = "black", linewidth = 0.45) +  # Línea horizontal en 0
  scale_x_continuous(breaks = 1:12) +  # Mostrar enteros de 1 a 12 en el eje X
  scale_color_manual(values = c("#3366FF", "#00DFA2", "#E6004C")) +  # Colores personalizados
  scale_shape_manual(values = shape_values) +  # Aplicar formas personalizadas
  labs(
    x = NULL,
    y = NULL,
    title = NULL,
    color = NULL,
    shape = NULL  # Ocultar título de la leyenda de formas
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
  )

# Mostrar el gráfico
horizon_plot


# Guardar el gráfico
plot_output_file <- file.path(output_dir, "releses_horizon_plot.png")
ggsave(filename = plot_output_file, plot = horizon_plot, width = 10, height = 6, dpi = 300, bg = "white")

