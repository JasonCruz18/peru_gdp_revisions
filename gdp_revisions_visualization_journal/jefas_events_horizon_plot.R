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
# Data Preparation (1st graph)
#*******************************************************************************
df_1 <- df %>%
  mutate(
    target_period = gsub("m", "-", target_period),
    target_period = as.yearmon(target_period, "%Y-%m"),
    target_period = as.Date(target_period),
    horizon = as.numeric(horizon)
  )

selected_vintages_1 <- as.Date(c("2006-05-01", "2012-06-01"))

df_filtered_1_1 <- df %>%
  filter(target_period %in% selected_vintages_1) %>%
  mutate(
    vintage_label = case_when(
      target_period == as.Date("2006-05-01") ~ "2006m05",
      target_period == as.Date("2012-06-01") ~ "2012m06",
      TRUE ~ as.character(target_period)
    )
  )

# Wide → Long: add both release & release_hat
df_long_1 <- df_filtered_1 %>%
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
# Visualization (fixed legend labels)
#*******************************************************************************

color_values_1 <- c(
  "2006m05" = "#3366FF", "2012m07" = "#00DFA2",
  "2006m05 now" = "#3366FF", "2012m07 now" = "#00DFA2"
)

shape_values_1 <- c(
  "2006m05" = 16, "2012m07" = 15
)

legend_labels_1 <- c(
  "2006m05" = "May 2006",
  "2012m07" = "July 2012"
)

horizon_plot_1 <- ggplot(df_long, aes(x = horizon, y = value, color = series_label)) +
  # lines for all (solid for releases, dashed for nowcasts)
  geom_line(aes(alpha = ifelse(type == "nowcast", 0.75, 1),
                linetype = ifelse(type == "nowcast", "dashed", "solid")),
            linewidth = 1.2) +
  # points only for releases
  geom_point(data = subset(df_long, type == "release"),
             aes(shape = series_label), size = 4.0) +
  scale_x_continuous(breaks = 1:12) +
  scale_color_manual(values = color_values, labels = legend_labels_1) +
  scale_shape_manual(values = shape_values, labels = legend_labels_1) +
  scale_alpha_identity() +
  scale_linetype_identity() +
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  guides(
    color = guide_legend(
      nrow = 1, byrow = TRUE,
      keywidth = 2, keyheight = 1.2,   # make boxes larger
      override.aes = list(
        shape = c(16, 15),             # only releases
        linetype = c("solid","solid"), # solid for release lines in legend
        alpha = c(1,1),                # full alpha
        size = c(4,4)
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

horizon_plot_1

# Save high-resolution PNG (300 DPI)
ggsave(filename = file.path(output_dir, "Fig_Nwc_1.png"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9, 
       dpi = 300,       # Set DPI to 300 for high resolution
       bg = "white")

# Save as high-resolution PDF
ggsave(filename = file.path(output_dir, "Fig_Nwc_1.pdf"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9)

# Save plot as EPS file
ggsave(filename = file.path(output_dir, "Fig_Nwc_1.eps"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9, 
       device = "eps",      # Set device to EPS
       dpi = 300,           # Ensure high resolution (300 DPI)
       bg = "white")        # White background


#*******************************************************************************
# Data Preparation (2nd graph)
#*******************************************************************************
df_2 <- df %>%
  mutate(
    target_period = gsub("m", "-", target_period),
    target_period = as.yearmon(target_period, "%Y-%m"),
    target_period = as.Date(target_period),
    horizon = as.numeric(horizon)
  )

selected_vintages_2 <- as.Date(c("2015-08-01", "2018-07-01"))

df_filtered_2 <- df %>%
  filter(target_period %in% selected_vintages_2) %>%
  mutate(
    vintage_label = case_when(
      target_period == as.Date("2015-08-01") ~ "2015m08",
      target_period == as.Date("2018-07-01") ~ "2018m07",
      TRUE ~ as.character(target_period)
    )
  )

# Wide → Long: add both release & release_hat
df_long_2 <- df_filtered_2 %>%
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
color_values_2 <- c("2015m08" = "#3366FF", "2018m07" = "#E6004C",
                    "2015m08 now" = "#3366FF", "2018m07 now" = "#E6004C")

shape_values_2 <- c("2015m08" = 15, "2018m07" = 17)

horizon_plot_2 <- ggplot(df_long_2, aes(x = horizon, y = value, color = series_label)) +
  # lines for all (solid for releases, dashed for nowcasts)
  geom_line(aes(alpha = ifelse(type == "nowcast", 0.75, 1),
                linetype = ifelse(type == "nowcast", "dashed", "solid")),
            linewidth = 1.2) +
  # points only for releases
  geom_point(data = subset(df_long_2, type == "release"),
             aes(shape = series_label), size = 4.0) +
  scale_x_continuous(breaks = 1:12) +
  scale_color_manual(values = color_values_2,
                     breaks = c("2015m08","2018m07",
                                "2015m08 now","2018m07 now")) +
  scale_shape_manual(values = shape_values_2) +
  scale_alpha_identity() +
  scale_linetype_identity() +
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  guides(
    color = guide_legend(
      nrow = 2, byrow = TRUE,
      keywidth = 1.5, keyheight = 1.2,   # make boxes larger
      override.aes = list(
        shape = c(15, 17),             # shapes only for releases
        linetype = c("solid","solid"),      # dashed for nowcasts
        alpha = c(1,1),                  # transparency
        size = c(4,4)              # thicker lines & bigger symbols
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

horizon_plot_2

# Save high-resolution PNG (300 DPI)
ggsave(filename = file.path(output_dir, "Fig_Nwc_2.png"), 
       plot = horizon_plot_2, 
       width = 16, 
       height = 9, 
       dpi = 300,       # Set DPI to 300 for high resolution
       bg = "white")

# Save as high-resolution PDF
ggsave(filename = file.path(output_dir, "Fig_Nwc_2.pdf"), 
       plot = horizon_plot_2, 
       width = 16, 
       height = 9)

# Save plot as EPS file
ggsave(filename = file.path(output_dir, "Fig_Nwc_2.eps"), 
       plot = horizon_plot_2, 
       width = 16, 
       height = 9, 
       device = "eps",      # Set device to EPS
       dpi = 300,           # Ensure high resolution (300 DPI)
       bg = "white")        # White background
