# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

# Visual quality control of raw location data

# Compiled by: David Green
# Date: 17 May 2022

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
library(tidyverse)
library(here)
library(data.table)
library(fs)
library(rnaturalearthdata)
# library(cowplot)
library(gridExtra)
# Read in location data
i_am("./Visual_qc_loc_data.R") # set location of root directory
locData <- fread(here("Consolidated_datasets/loc_all_ses_raw_pre-qc.txt"))

# Creating some qc plots, iterating through individuals
dir_create(here("Consolidated_datasets/loc_qc_plots/")) # Create directory to save qc plot files

ext <- c(-180, 180, -90, -30)
wrld <- countries50 |> 
  fortify()

#
lapply(unique(locData$REF), function(x){
# x = unique(locData$REF)[1]
# ext <- c(range(locData$LON, na.rm=T),
#          range(locData$LAT, na.rm=T))


subDat <- locData |> 
  filter(REF %in% x)

# Simple track map
g_track <- ggplot(data = subDat,
                  mapping = aes(x = LON,
                                y = LAT,
                                colour = D_DATE
                                )
                  ) +
  geom_polygon(mapping = aes(x = long,
                             y = lat,
                             group = group),
               data = wrld,
               fill = "grey60",
               colour = NA) +
  geom_path() + 
  coord_fixed(xlim = ext[1:2],
              ylim = ext[3:4],
              expand = FALSE,
              ratio = 2
              )
  
# g_track

# Longitude against time
g_long <- ggplot(data = subDat,
                 mapping = aes(x = D_DATE,
                               y = LON)
                 ) +
  geom_path() +
  geom_point()

# g_long

# Latitude against time
g_lat <- ggplot(data = subDat,
                 mapping = aes(x = D_DATE,
                               y = LAT)
) +
  geom_path() +
  geom_point()

# g_lat

# Tag metrics
time1 <- range(subDat$D_DATE)[1]
timeN <- range(subDat$D_DATE)[2]
tagDur <- paste(round(diff(range(subDat$D_DATE))), 
                     "days")
rInt <- subDat |>
  summarise(
    tInt = round(
      range(difftime(D_DATE,lag(D_DATE), 
            units = "hours"), 
           na.rm=T)
      )
    )

plotDat <- tibble(
  Metric = c("Start date", "End date", "Duration", "Min fix interval", "Max fix interval"),
  Value = c(format(time1, "%Y-%m-%d"), 
            format(timeN, "%Y-%m-%d"), 
            tagDur, 
            paste(rInt[1,1], "hours"), 
            paste(rInt[2,1], "hours"))
)

# plot_grid(g_track, plot_grid(g_long, g_lat, ncol = 2), NULL, ncol = 1)

ggsave(filename = paste0(here("Consolidated_datasets/loc_qc_plots/"),"/", x,"qc.pdf"),
       plot = grid.arrange(grobs = list(g_track, 
                                        grid.arrange(g_long, g_lat, ncol = 2), 
                                        tableGrob(plotDat)), 
                           ncol = 1,
                           top = paste("Deployment: ", x)),
        device = "pdf",
        height = 29.7,
        width = 21,
        units = "cm"
        )
})
