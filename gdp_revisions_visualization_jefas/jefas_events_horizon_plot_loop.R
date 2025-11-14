#*******************************************************************************
# Lines: Specific Annual Events by Horizon — Pairwise Automation
#*******************************************************************************
#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: forecasts_events_by_horizon_a.R
# + First Created: 11/10/24
# + Last Updated: 09/06/25
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

output_dir <- file.path(getwd(), "charts")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
cat("Directories created successfully in:", output_dir, "\n")

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

# Create a clean vintage label once (e.g., "2008m07")
df <- df %>%
  mutate(
    vintage_label_base = format(target_period, "%Ym%m")
  )

#*******************************************************************************
# Helper: safe palette generator (distinct hues)
#*******************************************************************************
make_palette <- function(n) {
  if (n <= 8) {
    # a pleasant qualitative palette up to 8 items
    base <- c("#3366FF","#00DFA2","#E6004C","#FF9900","#8A2BE2","#228B22","#FF1493","#1F78B4")
    return(base[seq_len(n)])
  } else {
    # fallback to evenly spaced hues
    hues <- seq(15, 375, length.out = n + 1)
    hcl(h = hues, l = 65, c = 100)[1:n]
  }
}

#*******************************************************************************
# Build consecutive, non-overlapping pairs of unique target periods
#*******************************************************************************
all_vintages <- df %>%
  distinct(target_period, vintage_label_base) %>%
  arrange(target_period)

n_vintages <- nrow(all_vintages)

if (n_vintages < 2) {
  stop("Not enough target periods to form pairs.")
}

if (n_vintages %% 2 == 1) {
  warning("Odd number of target periods. The last one will be ignored to keep non-overlapping pairs.")
  all_vintages <- all_vintages %>% slice(1:(n_vintages - 1))
}

# Number of charts to produce equals number of pairs
n_pairs <- nrow(all_vintages) / 2
cat("Total target periods:", nrow(all_vintages), "-> charts to be generated:", n_pairs, "\n")

#*******************************************************************************
# Global axis limits (based on all data)
#*******************************************************************************
hmin <- df %>% pull(horizon) %>% min(na.rm = TRUE)
hmax <- df %>% pull(horizon) %>% max(na.rm = TRUE)

#*******************************************************************************
# Loop over pairs and generate charts
#*******************************************************************************
for (i in seq_len(n_pairs)) {
  
  #----------------------------------------------------------------------------#
  # Select the i-th pair (non-overlapping consecutive)
  #----------------------------------------------------------------------------#
  idx1 <- (i - 1) * 2 + 1
  idx2 <- idx1 + 1
  vint_pair <- all_vintages %>% slice(idx1, idx2)
  
  pair_dates <- vint_pair$target_period
  pair_labels <- vint_pair$vintage_label_base
  
  #----------------------------------------------------------------------------#
  # Filter data to the pair and build labels for release/nowcast
  #----------------------------------------------------------------------------#
  df_filtered <- df %>%
    filter(target_period %in% pair_dates) %>%
    mutate(
      vintage_label = case_when(
        target_period == pair_dates[1] ~ pair_labels[1],
        target_period == pair_dates[2] ~ pair_labels[2],
        TRUE ~ vintage_label_base
      )
    )
  
  # Long format with both release & nowcast
  df_long <- df_filtered %>%
    select(horizon, vintage_label, gdp_release, gdp_release_hat) %>%
    pivot_longer(
      cols = c("gdp_release", "gdp_release_hat"),
      names_to = "series", values_to = "value"
    ) %>%
    mutate(
      series_label = ifelse(series == "gdp_release",
                            vintage_label,
                            paste0(vintage_label, " now")),
      type = ifelse(series == "gdp_release", "release", "nowcast")
    )
  
  #----------------------------------------------------------------------------#
  # Aesthetics: colors & shapes (auto from pair)
  #----------------------------------------------------------------------------#
  releases <- pair_labels
  nowcasts <- paste0(pair_labels, " now")
  legend_order <- c(releases, nowcasts)
  
  # Colors: same color for release & its nowcast
  base_cols <- make_palette(length(pair_labels))
  names(base_cols) <- pair_labels
  color_values <- c(base_cols, setNames(base_cols, nowcasts))
  
  # Distinct shapes for releases
  shape_pool <- c(16, 15, 17, 18, 8, 4, 3, 7)
  shape_values <- setNames(shape_pool[seq_along(pair_labels)], pair_labels)
  
  #----------------------------------------------------------------------------#
  # Plot
  #----------------------------------------------------------------------------#
  horizon_plot <- ggplot(df_long, aes(x = horizon, y = value, color = series_label)) +
    # lines for all (solid for releases, dashed for nowcasts)
    geom_line(aes(alpha = ifelse(type == "nowcast", 0.75, 1),
                  linetype = ifelse(type == "nowcast", "dashed", "solid")),
              linewidth = 1.2) +
    # points only for releases
    geom_point(
      data = subset(df_long, type == "release"),
      aes(shape = series_label), size = 4.0
    ) +
    scale_x_continuous(breaks = seq(hmin, hmax, by = 1), limits = c(hmin, hmax)) +
    scale_color_manual(values = color_values, breaks = legend_order) +
    scale_shape_manual(values = shape_values) +
    scale_alpha_identity() +
    scale_linetype_identity() +
    labs(x = NULL, y = NULL, title = NULL, color = NULL) +
    guides(
      color = guide_legend(
        nrow = 2, byrow = TRUE,
        keywidth = 1.5, keyheight = 1.2,
        override.aes = list(
          shape    = c(rep(16, length(releases)), rep(NA, length(nowcasts))),
          linetype = c(rep("solid", length(releases)), rep("dashed", length(nowcasts))),
          alpha    = c(rep(1, length(releases)), rep(0.75, length(nowcasts))),
          size     = rep(4, length(legend_order))
        )
      ),
      shape = "none"
    ) +
    theme_minimal() +
    theme(
      panel.grid.major   = element_line(color = "#F5F5F5", linewidth = 0.8),
      panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 0.8),
      panel.grid.minor.y = element_blank(),
      axis.text          = element_text(color = "black", size = 16),
      axis.text.x        = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
      axis.text.y        = element_text(color = "black", angle = 0, hjust = 0.5),
      axis.ticks         = element_line(color = "black"),
      axis.ticks.length  = unit(0.1, "inches"),
      axis.title.x       = element_blank(),
      axis.title.y       = element_text(size = 16, color = "black"),
      plot.title         = element_blank(),
      legend.position    = "bottom",
      legend.title       = element_blank(),
      legend.text        = element_text(size = 16, color = "black"),
      legend.background  = element_rect(fill = "white", color = "black", linewidth = 0.45),
      axis.line          = element_line(color = "black", linewidth = 0.45),
      panel.border       = element_rect(color = "black", linewidth = 0.45, fill = NA),
      plot.margin        = margin(9, 5, 9, 4)
    )
  
  #----------------------------------------------------------------------------#
  # Save with “_#” suffix (iteration index)
  #----------------------------------------------------------------------------#
  plot_output_file <- file.path(
    output_dir,
    sprintf("nowcasts_releases_%d.png", i)
  )
  ggsave(filename = plot_output_file, plot = horizon_plot, width = 10, height = 6, dpi = 300, bg = "white")
  
  cat(sprintf("Saved chart %d of %d: %s\n", i, n_pairs, plot_output_file))
}

cat("All charts generated successfully in:", output_dir, "\n")
