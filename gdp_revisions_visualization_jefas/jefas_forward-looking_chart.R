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
query <- "SELECT * FROM jefas_gdp_revisions_base_year_panel"
df <- dbGetQuery(con, query)

# Close database connection
dbDisconnect(con)



#*******************************************************************************
# 1. Suavizado de la serie gdp_release
#*******************************************************************************

# Suavizado ponderado (ventana: t-2 a t+2)
df <- df %>%
  mutate(
    gdp_release_smooth = (
      lag(gdp_release, 2) +
        2 * lag(gdp_release, 1) +
        4 * gdp_release +
        2 * lead(gdp_release, 1) +
        lead(gdp_release, 2)
    ) / 10
  )

#*******************************************************************************
# 2. Preparación de datos
#*******************************************************************************

# Conversión de tipos
df <- df %>%
  mutate(
    vintages_date = as.Date(vintages_date),
    horizon = as.numeric(horizon)
  )

# Remover valores anómalos del período COVID y 2007 base year
df <- df %>%
  mutate(
    gdp_release = ifelse(
      vintages_date >= as.Date("2020-03-01") & vintages_date <= as.Date("2021-10-01"),
      NaN, gdp_release
    ),
    gdp_release_smooth = ifelse(
      vintages_date >= as.Date("2020-03-01") & vintages_date <= as.Date("2021-10-01"),
      NaN, gdp_release_smooth
    )
  )

# Filtrar solo meses clave: enero, abril, julio, octubre (desde 1993)
df_filtered_with_smooth <- df %>%
  filter(
    lubridate::month(vintages_date) %in% c(1, 4, 7, 10),
    vintages_date >= as.Date("2001-01-01"),
    vintages_date <= as.Date("2023-10-31")
  ) %>%
  mutate(
    release_date = vintages_date + months(horizon - 1),
    target_year = year(vintages_date)
  )

# Subconjunto para h = 1 (primer release de cada vintage)
df_h1 <- df %>%
  filter(horizon == 1) %>%
  mutate(
    release_date = vintages_date + months(horizon - 1)
  )

#*******************************************************************************
# 3. Ajuste vertical de líneas originales para alinear con valor suavizado
#*******************************************************************************

# Calcular el valor de ajuste para alinear el primer punto de cada línea azul
adjustment_df <- df_filtered_with_smooth %>%
  group_by(vintages_date) %>%
  filter(row_number() == 1) %>%
  mutate(adjustment = gdp_release_smooth - gdp_release) %>%
  select(vintages_date, adjustment)

# Aplicar ajuste a toda la serie de cada vintage
df_adjusted <- df_filtered_with_smooth %>%
  left_join(adjustment_df, by = "vintages_date") %>%
  mutate(gdp_release_adjusted = gdp_release + adjustment)

#*******************************************************************************
# 4. Visualización: Revisiones del PBI por horizonte (release 1–12)
#*******************************************************************************

plot <- ggplot() +
  
  # Regiones sombreadas
  geom_rect(aes(xmin = as.Date("2013-01-01"), xmax = as.Date("2014-01-01"),
                ymin = -Inf, ymax = Inf, fill = "2007 base year"), alpha = 0.65) +
  geom_rect(aes(xmin = as.Date("2020-03-01"), xmax = as.Date("2021-10-01"),
                ymin = -Inf, ymax = Inf, fill = "COVID-19"), alpha = 0.65) +
  
  # Línea principal: 1st release suavizado (línea negra)
  geom_line(
    data = df_h1,
    aes(x = release_date, y = gdp_release_smooth, color = "1st release"),
    linewidth = 0.5
  ) +
  geom_point(
    data = df_h1,
    aes(x = release_date, y = gdp_release_smooth, color = "1st release"),
    size = 0.85
  ) +
  
  # Línea secundaria: valores ajustados
  geom_line(
    data = df_adjusted,
    aes(x = release_date, y = gdp_release_adjusted, group = vintages_date,
        color = "Ongoing releases"),
    linewidth = 0.85,
    alpha = 0.75
  ) +
  
  labs(
    x = NULL,
    y = "GDP Releases",
    title = NULL,
    color = NULL,
    fill = NULL
  ) +
  
  scale_x_date(
    breaks = seq(as.Date("2001-01-01"), as.Date("2025-01-01"), by = "2 years"),
    date_labels = "%Y",
    limits = c(as.Date("2001-01-01"), as.Date("2025-01-01")),
    expand = c(0.02, 0.02)
  ) +
  scale_y_continuous(
    breaks = scales::pretty_breaks(n = 5),
    labels = scales::number_format(accuracy = 0.1)
  ) +
  
  scale_color_manual(
    values = c("1st release" = "#292929", "Ongoing releases" = "#0079FF")
  ) +
  scale_fill_manual(
    values = c("COVID-19" = "#F6FA70", "2007 base year" = "#00DFA2")
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
  coord_cartesian(ylim = c(-3.0, 12.3), clip = "off")

print(plot)

# Guardar
ggsave(filename = file.path(output_dir, "gdp_revisions_by_horizon_events.png"), plot = plot, width = 12, height = 8)


