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

# Convertir a tipos adecuados
df <- df %>%
  mutate(
    vintages_date = as.Date(vintages_date),
    horizon = as.numeric(horizon)
  )

# Filtrar solo los marzos y calcular la fecha de publicación del release
df_filtered <- df %>%
  filter(month(vintages_date) == 3) %>%
  mutate(
    release_date = vintages_date + months(horizon - 1),
    target_year = year(vintages_date)
  )


#*******************************************************************************
# Plot: GDP Revisions by Horizon for March Events (con línea h=1 destacada)
#*******************************************************************************

plot <- ggplot() +
  # Todas las líneas: eventos (con transparencia y color uniforme)
  geom_line(
    data = df_filtered,
    aes(x = release_date, y = gdp_release, group = vintages_date, color = "Event year (March)"),
    linewidth = 1.2,  # Grosor de línea similar al gráfico de referencia
    alpha = 0.5
  ) +
  
  # Línea especial para h = 1 (primer release)
  geom_line(
    data = subset(df_filtered, horizon == 1),
    aes(x = release_date, y = gdp_release, color = "1st release"),  # Etiqueta para la leyenda
    linewidth = 1.8,  # Grosor de línea similar al gráfico de referencia
    alpha = 1  # Aumentar opacidad para que sea más visible
  ) +
  
  # Puntos para los eventos
  geom_point(
    data = df_filtered,
    aes(x = release_date, y = gdp_release, group = vintages_date, color = "Event year (March)"),
    size = 1.6,  # Tamaño de los puntos como en el gráfico de referencia
    alpha = 0.8
  ) +
  
  # Títulos y ejes
  labs(
    title = "GDP Revisions by Horizon (Release 1–12)",
    subtitle = "Target GDP: every March between 1993 and 2023",
    x = "Release date",  # Etiqueta del eje X
    y = "GDP growth rate (%)"
  ) +
  scale_x_date(
    date_labels = "%Y",  # Solo mostrar el año
    date_breaks = "2 year", 
    limits = c(min(df_filtered$release_date), max(df_filtered$release_date))  # Establecer límites explícitos
  ) +
  scale_y_continuous(
    limits = c(min(df_filtered$gdp_release, na.rm = TRUE), max(df_filtered$gdp_release, na.rm = TRUE))  # Establecer límites del eje y
  ) +
  scale_color_manual(
    values = c("1st release" = "#292929", "Event year (March)" = "#0079FF")  # Colores personalizados
  ) +
  theme_minimal(base_size = 14) +
  theme(
    panel.grid.major = element_line(color = "#F5F5F5", linewidth = 0.8),  # Rejillas de fondo
    panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.y = element_blank(),  # Eliminar la rejilla menor en el eje Y
    axis.text = element_text(color = "black", size = 16),  # Tamaño de texto en ejes
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),  # Texto horizontal en eje X
    axis.text.y = element_text(color = "black", size = 16),  # Tamaño de texto en eje Y
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_text(size = 16, color = "black"),  # Etiqueta del eje X
    axis.title.y = element_text(size = 16, color = "black"),  # Título del eje Y
    plot.title = element_text(face = "bold"),  # Título en negrita
    plot.subtitle = element_text(size = 14, color = "black"),  # Subtítulo con color y tamaño
    legend.position = "top",  # Posición de la leyenda
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),  # Borde del gráfico
    plot.margin = margin(9, 5, 9, 4)  # Márgenes
  ) +
  guides(color = guide_legend(title = "Legend"))  # Título para la leyenda

# Mostrar
print(plot)




# Guardar
ggsave(filename = file.path(output_dir, "gdp_revisions_by_horizon_events.png"), plot = plot, width = 12, height = 8)


