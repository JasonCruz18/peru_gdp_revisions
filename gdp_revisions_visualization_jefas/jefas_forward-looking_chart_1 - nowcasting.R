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
# 1. Suavizado de las series
#*******************************************************************************

df <- df %>%
  mutate(
    gdp_release_smooth = (
      lag(gdp_release, 2) +
        2 * lag(gdp_release, 1) +
        4 * gdp_release +
        2 * lead(gdp_release, 1) +
        lead(gdp_release, 2)
    ) / 10,
    gdp_release_hat_smooth = (
      lag(gdp_release_hat, 2) +
        2 * lag(gdp_release_hat, 1) +
        4 * gdp_release_hat +
        2 * lead(gdp_release_hat, 1) +
        lead(gdp_release_hat, 2)
    ) / 10
  )

#*******************************************************************************
# 2. Preparación de datos
#*******************************************************************************

df <- df %>%
  mutate(
    target_period = gsub("m", "-", target_period),
    target_period = as.yearmon(target_period, "%Y-%m"),
    target_period = as.Date(target_period),
    horizon = as.numeric(horizon)
  )

df <- df %>%
  mutate(
    gdp_release = ifelse(
      target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"),
      NA_real_, gdp_release
    ),
    gdp_release_smooth = ifelse(
      target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"),
      NA_real_, gdp_release_smooth
    ),
    gdp_release_hat = ifelse(
      target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"),
      NA_real_, gdp_release_hat
    ),
    gdp_release_hat_smooth = ifelse(
      target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"),
      NA_real_, gdp_release_hat_smooth
    )
  )


df_filtered_with_smooth <- df %>%
  filter(
    lubridate::month(target_period) %in% c(2, 4, 6, 8, 10, 12),
    target_period >= as.Date("2001-01-01"),
    target_period <= as.Date("2023-10-31")
  ) %>%
  mutate(
    release_date = target_period + months(horizon - 1),
    target_year = year(target_period)
  )

df_h1 <- df %>%
  filter(horizon == 1) %>%
  mutate(
    release_date = target_period + months(horizon - 1)
  )

#*******************************************************************************
# 3. Ajuste vertical para ambas series
#*******************************************************************************

adjustment_df <- df_filtered_with_smooth %>%
  group_by(target_period) %>%
  filter(!is.na(gdp_release) & !is.na(gdp_release_smooth)) %>%
  slice(1) %>%
  mutate(
    adjustment_release = gdp_release_smooth - gdp_release,
    adjustment_hat = gdp_release_hat_smooth - gdp_release_hat
  )

df_adjusted <- df_filtered_with_smooth %>%
  left_join(
    adjustment_df %>%
      select(target_period, adjustment_release, adjustment_hat),
    by = "target_period"
  ) %>%
  mutate(
    gdp_release_adjusted = gdp_release + adjustment_release,
    gdp_release_hat_adjusted = gdp_release_hat + adjustment_hat
  )

# Últimos puntos para ambos
df_adjusted_last_points <- df_adjusted %>%
  group_by(target_period) %>%
  filter(release_date == max(release_date)) %>%
  ungroup()

#*******************************************************************************
# 4. Visualización
#*******************************************************************************

plot <- ggplot() +
  
  # Regiones sombreadas
  geom_rect(aes(xmin = as.Date("2013-01-01"), xmax = as.Date("2014-01-01"),
                ymin = -Inf, ymax = Inf, fill = "2007 base year"), alpha = 0.70) +
  geom_rect(aes(xmin = as.Date("2020-03-01"), xmax = as.Date("2021-10-01"),
                ymin = -Inf, ymax = Inf, fill = "COVID-19"), alpha = 0.70) +
  
  # Línea principal: 1st release suavizado (línea negra)
  geom_line(
    data = df_h1,
    aes(x = release_date, y = gdp_release_smooth, color = "1st release"),
    linewidth = 0.5
  ) +
  geom_point(
    data = df_h1,
    aes(x = release_date, y = gdp_release_smooth, color = "1st release"),
    shape = 21,
    size = 0.85,
    stroke = 0.85,
    fill = NA
  ) +
  
  # Línea secundaria: Ongoing releases reales
  geom_line(
    data = df_adjusted,
    aes(x = release_date, y = gdp_release_adjusted, group = target_period,
        color = "Ongoing releases"),
    linewidth = 0.85,
    alpha = 0.70
  ) +
  
  # NUEVO: Ongoing releases (hat)
  geom_line(
    data = df_adjusted,
    aes(x = release_date, y = gdp_release_hat_adjusted, group = target_period,
        color = "Ongoing releases (hat)"),
    linewidth = 1.25,
    alpha = 0.70,
    linetype = "solid"
  ) +
  
  # Últimos puntos (reales)
  geom_point(
    data = df_adjusted_last_points,
    aes(x = release_date, y = gdp_release_adjusted, color = "Last release"),
    shape = 15,
    size = 1.70
  ) +
  
  # Últimos puntos (hat)
  geom_point(
    data = df_adjusted_last_points,
    aes(x = release_date, y = gdp_release_hat_adjusted, color = "Last release (hat)"),
    shape = 17,
    size = 1.70
  ) +
  
  labs(
    x = NULL,
    y = NULL,
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
    values = c(
      "1st release" = "#3366FF",
      "Ongoing releases" = "#E6004C",
      "Last release" = "#E6004C",
      "Ongoing releases (hat)" = "purple",
      "Last release (hat)" = "purple"
    ),
    breaks = c("1st release", "Ongoing releases", "Ongoing releases (hat)",
               "Last release", "Last release (hat)")
  ) +
  
  scale_fill_manual(
    values = c("COVID-19" = "#FFF183", "2007 base year" = "#00DFA2")
  ) +
  
  guides(
    color = guide_legend(order = 1),
    fill = guide_legend(order = 2)
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
  coord_cartesian(ylim = range(c(df_adjusted$gdp_release_adjusted,
                                 df_adjusted$gdp_release_hat_adjusted), na.rm = TRUE) * c(1.05, 1.05))


print(plot)



# Guardar
ggsave(filename = file.path(output_dir, "gdp_revisions_by_horizon_events_1.png"), plot = plot, width = 12, height = 8, bg = "white")

