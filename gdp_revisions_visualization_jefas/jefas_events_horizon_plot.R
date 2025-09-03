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
library(RPostgres)
library(ggplot2)
library(lubridate)
library(svglite)
library(dplyr)
library(tidyr)
library(tcltk)
library(sandwich)
library(lmtest)
library(scales)
library(zoo)

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
# Import from CSV file
#*******************************************************************************
file_path <- "C:/Users/Jason Cruz/OneDrive/Documentos/jefas_nowcasting_panel.csv"
df <- read.csv(file_path)

#*******************************************************************************
# Data Preparation
#*******************************************************************************
df <- df %>%
  mutate(
    target_period = gsub("m", "-", target_period),
    target_period = as.yearmon(target_period, "%Y-%m"),
    target_period = as.Date(target_period),
    horizon = as.numeric(horizon)
  )

selected_vintages <- as.Date(c("2008-07-01", "2008-08-01", "2008-09-01"))

df_filtered <- df %>%
  filter(target_period %in% selected_vintages) %>%
  mutate(
    vintage_label = case_when(
      target_period == as.Date("2008-07-01") ~ "2008m07",
      target_period == as.Date("2008-08-01") ~ "2008m08",
      target_period == as.Date("2008-09-01") ~ "2008m09",
      TRUE ~ as.character(target_period)
    )
  )

# Wide → Long: add both release & release_hat
df_long <- df_filtered %>%
  select(horizon, vintage_label, gdp_release, gdp_release_hat) %>%
  pivot_longer(cols = c("gdp_release", "gdp_release_hat"),
               names_to = "series", values_to = "value") %>%
  mutate(
    series_label = case_when(
      series == "gdp_release" ~ vintage_label,
      series == "gdp_release_hat" ~ paste0(vintage_label, " now")
    ),
    type = ifelse(series == "gdp_release", "release", "nowcast")
  )

#*******************************************************************************
# Visualization
#*******************************************************************************
color_values <- c("2008m07" = "#3366FF", "2008m08" = "#00DFA2", "2008m09" = "#E6004C",
                  "2008m07 now" = "#3366FF", "2008m08 now" = "#00DFA2", "2008m09 now" = "#E6004C")

shape_values <- c("2008m07" = 16, "2008m08" = 15, "2008m09" = 17)

horizon_plot <- ggplot(df_long, aes(x = horizon, y = value, color = series_label)) +
  # lines for all (solid for releases, dashed for nowcasts)
  geom_line(aes(alpha = ifelse(type == "nowcast", 0.75, 1),
                linetype = ifelse(type == "nowcast", "dashed", "solid")),
            linewidth = 1.2) +
  # points only for releases
  geom_point(data = subset(df_long, type == "release"),
             aes(shape = series_label), size = 4.0) +
  scale_x_continuous(breaks = 1:12) +
  scale_color_manual(values = color_values,
                     breaks = c("2008m07","2008m08","2008m09",
                                "2008m07 now","2008m08 now","2008m09 now")) +
  scale_shape_manual(values = shape_values) +
  scale_alpha_identity() +
  scale_linetype_identity() +
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  guides(
    color = guide_legend(
      nrow = 2, byrow = TRUE,
      keywidth = 1.5, keyheight = 1.2,   # make boxes larger
      override.aes = list(
        shape = c(16, 15, 17, NA, NA, NA),             # shapes only for releases
        linetype = c("solid","solid","solid",
                     "dashed","dashed","dashed"),      # dashed for nowcasts
        alpha = c(1,1,1,0.75,0.75,0.75),                  # transparency
        size = c(4,4,4,4,4,4)              # thicker lines & bigger symbols
      )
    ),
    shape = "none"
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

horizon_plot

plot_output_file <- file.path(output_dir, "nowcasts_releases_horizon_plot_1.png")
ggsave(filename = plot_output_file, plot = horizon_plot, width = 10, height = 6, dpi = 300, bg = "white")
