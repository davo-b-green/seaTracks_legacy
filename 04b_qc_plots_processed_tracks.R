# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

# Visual quality control of raw location data

# Compiled by: David Green
# Date: 28 June 2022

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
library(tidyverse)
library(here) # This sets location of root directory
library(data.table)
library(fs)
library(rnaturalearthdata)
# library(cowplot)
library(gridExtra)
library(future.apply)

# Read in location data
load(file = "/processed_datasets/ssm_6h.Rdata")

# Create output directory if it doesn't already exist
dir_create(here("./data_qc_plots/loc_ssm_qc_plots/")) # Create directory to save qc plot files

wrld <- countries50 %>% 
  fortify()

# Creating some qc plots, iterating through individuals
par.ls <- split(unique(ssm_6h$id), cut(seq_along(unique(ssm_6h$id)), 104, labels = FALSE))
plan("future::multisession")

sfp <- future_lapply(par.ls, function(z) {
  
  lapply(z, function(ii){ # Iterate through individual deployments
    subDat <- ssm_6h %>%
      filter(id %in% ii)
    
    subraw <- d1 %>% 
      filter(id %in% ii)
    
    xlim = range(subDat$lon, na.rm=T)
    ylim = range(subDat$lat, na.rm=T)
    
    # Simple track map
    g_track <- ggplot(data = subDat,
                      mapping = aes(x = lon,
                                    y = lat
                                    )
                      ) +
      geom_polygon(mapping = aes(x = long,
                                 y = lat,
                                 group = group),
                   data = wrld,
                   fill = "grey60",
                   colour = NA) +
      geom_path(mapping = aes(x = lon,
                              y = lat
                              ),
                colour = "#B35806",# "#542788"
                size = 0.5,
                data = subraw) +
      geom_path(colour = "#542788",
                size = 0.5) + 
      coord_fixed(xlim = xlim,
                  ylim = ylim,
                  expand = TRUE,
                  ratio = 2
                  )
    # g_track
    # Longitude against time
    g_long <- ggplot(data = subDat,
                     mapping = aes(x = date,
                                   y = lon)
                     ) +
      geom_path(mapping = aes(x = date,
                              y = lon
      ),
      colour = "#B35806",
      size = 0.5,
      data = subraw) +
      # geom_path(colour = "#542788",
      #           size = 0.5) +
      geom_path(colour = "#542788",
                 size = 0.5)
    # g_long
    # Latitude against time
    g_lat <- ggplot(data = subDat,
                     mapping = aes(x = date,
                                   y = lat)
    ) +
     geom_path(mapping = aes(x = date,
                              y = lat
      ),
      colour = "#B35806",
      size = 0.5,
      data = subraw) +
      # geom_path(colour = "#542788",
      #           size = 0.5) +
      geom_path(colour = "#542788",
                 size = 0.5)
    # g_lat
    # Tag metrics
    time1 <- range(subDat$date)[1]
    timeN <- range(subDat$date)[2]
    tagDur <- paste(round(diff(range(subDat$date))), 
                         "days")
    rInt <- subDat %>%
      summarise(
        tInt = round(
          range(difftime(date,lag(date), 
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
    ga <- grid.arrange(grobs = list(g_track, 
                                    grid.arrange(g_long, g_lat, ncol = 2), 
                                    tableGrob(plotDat)), 
                       ncol = 1,
                       top = paste("Deployment: ", ii))
    ggsave(filename = paste0(here("./data_qc_plots/loc_ssm_qc_plots/"),"/", ii,"_qc.pdf"),
           plot = ga,
            device = "pdf",
            height = 29.7,
            width = 21,
            units = "cm"
            )
  })
})

plan(sequential)