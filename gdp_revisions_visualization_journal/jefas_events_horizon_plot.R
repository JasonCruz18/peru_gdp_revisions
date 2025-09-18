
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

df_filtered_1 <- df %>%
  mutate(
    target_period = as.Date(paste(substr(target_period, 1, 4), substr(target_period, 6, 7), "01", sep = "-")),
    vintage_label = case_when(
      target_period == as.Date("2006-05-01") ~ "2006m05",
      target_period == as.Date("2012-06-01") ~ "2012m06",
      TRUE ~ as.character(target_period)
    )
  ) %>%
  filter(target_period %in% selected_vintages_1)

# Check the data again
head(df_filtered_1)


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
# Visualization (2006m05 and 2012m07)
#*******************************************************************************

# Limpieza preventiva (evita espacios/trailing mismatches)
df_long_1$series_label <- trimws(as.character(df_long_1$series_label))

# Colors: We need to include both the release and nowcast versions in color_values_1
color_values_1 <- c(
  "2006m05"     = "#3366FF",
  "2012m06"     = "#E6004C",
  "2006m05 now" = "#3366FF",  # Same color for nowcast of 2006m05
  "2012m06 now" = "#E6004C"   # Same color for nowcast of 2012m06
)

# Shapes: Shapes only for releases
shape_values_1 <- c(
  "2006m05" = 15,   # square
  "2012m06" = 17    # triangle
)

# Legend breaks based on available unique values in series_label
legend_breaks_1 <- c("2006m05", "2012m06")  # These are for releases only
legend_labels_1 <- c("May 2006", "June 2012")

# Create the plot with updated color and shape scales
horizon_plot_1 <- ggplot(df_long_1, aes(x = horizon, y = value, color = series_label, group = series_label)) +
  # Lines for both release and nowcast, dashed for nowcast, solid for release
  geom_line(aes(
    alpha    = ifelse(type == "nowcast", 0.75, 1),
    linetype = ifelse(type == "nowcast", "dashed", "solid")
  ), linewidth = 1.2) +
  
  # Points only for releases (shape mapped to series_label)
  geom_point(
    data = subset(df_long_1, type == "release"),
    aes(shape = series_label),
    size = 6.0
  ) +
  scale_x_continuous(breaks = 1:12) +
  scale_y_continuous(
    limits = c(6.5, 7.5),
    breaks = seq(6.5, 7.5, by = 0.25)
  ) +
  
  # Color and shape mappings
  scale_color_manual(
    values = color_values_1,
    breaks = legend_breaks_1,
    labels = legend_labels_1
  ) +
  scale_shape_manual(
    values = shape_values_1,
    breaks = legend_breaks_1,
    labels = legend_labels_1
  ) +
  scale_alpha_identity() +
  scale_linetype_identity() +
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  guides(
    # Only show the color legend, with overridden shape settings
    color = guide_legend(
      nrow = 1, byrow = TRUE,
      keywidth = 2, keyheight = 1.2,
      override.aes = list(
        shape    = unname(shape_values_1[legend_breaks_1]),
        linetype = rep("solid", length(legend_breaks_1)),
        alpha    = rep(1, length(legend_breaks_1)),
        size     = rep(4, length(legend_breaks_1))
      )
    ),
    shape = "none"  # Prevent the shape legend from being shown twice
  ) +
  
  theme_minimal() +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 28),
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = 28, color = "black"),
    plot.title = element_blank(),
    legend.position = c(0.97, 0.03),  # Lower right corner
    legend.justification = c("right", "bottom"),
    legend.title = element_blank(),
    legend.text = element_text(size = 28, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.45),
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),
    plot.margin = margin(9, 10, 9, 4)
  )

# Print the plot
print(horizon_plot_1)

# Save high-resolution PNG (300 DPI)
ggsave(filename = file.path(output_dir, "Fig_Nwc_1.png"), 
       plot = horizon_plot_1, 
       width = 10, 
       height = 10, 
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
  mutate(
    target_period = as.Date(paste(substr(target_period, 1, 4), substr(target_period, 6, 7), "01", sep = "-")),
    vintage_label = case_when(
      target_period == as.Date("2015-08-01") ~ "2015m08",
      target_period == as.Date("2018-07-01") ~ "2018m07",
      TRUE ~ as.character(target_period)
    )
  ) %>%
  filter(target_period %in% selected_vintages_2)

# Check the data again
head(df_filtered_2)


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
# Visualization (2015m08 and 2018m07)
#*******************************************************************************

# Limpieza preventiva (evita espacios/trailing mismatches)
df_long_2$series_label <- trimws(as.character(df_long_2$series_label))

# Colors: We need to include both the release and nowcast versions in color_values_2
color_values_2 <- c(
  "2015m08"     = "#3366FF",
  "2018m07"     = "#E6004C",
  "2015m08 now" = "#3366FF",  # Same color for nowcast of 2015m08
  "2018m07 now" = "#E6004C"   # Same color for nowcast of 2018m07
)

# Shapes: Shapes only for releases
shape_values_2 <- c(
  "2015m08" = 15,   # square
  "2018m07" = 17    # triangle
)

# Legend breaks based on available unique values in series_label
legend_breaks_2 <- c("2015m08", "2018m07")  # These are for releases only
legend_labels_2 <- c("August 2015", "July 2018")

# Create the plot with updated color and shape scales
horizon_plot_2 <- ggplot(df_long_2, aes(x = horizon, y = value, color = series_label, group = series_label)) +
  # Lines for both release and nowcast, dashed for nowcast, solid for release
  geom_line(aes(
    alpha    = ifelse(type == "nowcast", 0.75, 1),
    linetype = ifelse(type == "nowcast", "dashed", "solid")
  ), linewidth = 1.2) +
  
  # Points only for releases (shape mapped to series_label)
  geom_point(
    data = subset(df_long_2, type == "release"),
    aes(shape = series_label),
    size = 6.0
  ) +
  scale_x_continuous(breaks = 1:12) +
  scale_y_continuous(
    limits = c(2.35, 2.85),
    breaks = seq(2.35, 2.85, by = 0.10)
  ) +
  
  # Color and shape mappings
  scale_color_manual(
    values = color_values_2,
    breaks = legend_breaks_2,
    labels = legend_labels_2
  ) +
  scale_shape_manual(
    values = shape_values_2,
    breaks = legend_breaks_2,
    labels = legend_labels_2
  ) +
  scale_alpha_identity() +
  scale_linetype_identity() +
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  guides(
    # Only show the color legend, with overridden shape settings
    color = guide_legend(
      nrow = 1, byrow = TRUE,
      keywidth = 2, keyheight = 1.2,
      override.aes = list(
        shape    = unname(shape_values_2[legend_breaks_2]),
        linetype = rep("solid", length(legend_breaks_2)),
        alpha    = rep(1, length(legend_breaks_2)),
        size     = rep(4, length(legend_breaks_2))
      )
    ),
    shape = "none"  # Prevent the shape legend from being shown twice
  ) +
  
  theme_minimal() +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 28),
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = 28, color = "black"),
    plot.title = element_blank(),
    legend.position = c(0.97, 0.03),  # Lower right corner
    legend.justification = c("right", "bottom"),
    legend.title = element_blank(),
    legend.text = element_text(size = 28, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.45),
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),
    plot.margin = margin(9, 10, 9, 4)
  )

# Print the plot
print(horizon_plot_2)

# Save high-resolution PNG (300 DPI)
ggsave(filename = file.path(output_dir, "Fig_Nwc_2.png"), 
       plot = horizon_plot_2, 
       width = 10, 
       height = 10, 
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



