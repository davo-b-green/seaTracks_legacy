## Drift rate calulcations

## Author: David Green
## Date: 28-06-2022

library(tidyverse)
library(data.table)
library(nlme)
library(purrr)
library(drifteR)
library(rnaturalearthdata)
library(fs)
library(cmocean)
library(gridExtra)
library(future.apply)
# Create output directory if it doesn't already exist
dir_create("./data_qc_plots/d-rate_qc_plots/") # Create directory to save qc plot files

idmeta <- fread("IMOS_CTD_metadata.csv")
idmeta <- idmeta %>%
  dplyr::rename(id=SMRU_Ref)

wrld <- countries50 %>% 
  fortify()

dive <- fread("./processed_datasets/loc_ssm_dive.csv") %>% 
  left_join(idmeta, by = "id")
# plan("future::multisession")
# lapply(unique(dive$id), function(x){
for(x in unique(dive$id)[66:length(unique(dive$id))]){
# for(x in unique(dive$id)){
# x = unique(dive$id)[ii]  
dive2 <- filter(dive, id %in% x) %>% 
  select_if(!duplicated(toupper(names(.)))) %>% 
  rename_with(~ str_replace_all(.x, pattern = "\\_", replacement = ".")) %>% 
  rename_with(.cols = everything(), .fn = toupper) %>%  # Change all colnames to lowercase
  filter(D.MASK == 0)
if(nrow(dive2)==0){print("No dives passed initial qc check; skipping to next individual")}next
# if(nrow(dive2)==0)next

sdive <- rbs(dive2, num=NA)
sdive <- pDrift(sdive)

tstFit = tryCatch( # Catch any errors coming from there being no dive data
  fitDrate(sdive),
  error = function(e){ 
    message(paste("cannot compute drift rates for: ", x))
  })

if(is.null(tstFit))next

g1 <- ggplot() +
  geom_point(data = tstFit$data,
             aes(
               x = DE.DATE,
               y = drate,
               size = tstFit$w,
               alpha = tstFit$w
             ),
             show.legend = FALSE) +
  geom_ribbon(
    aes(
      x = tstFit$tSeq,
      ymin = tstFit$pred$lwr,
      ymax = tstFit$pred$upr
      ),
    inherit.aes = FALSE,
    alpha = 0.5,
    fill = "darkblue"
  ) +
  geom_path(
    data = tstFit$pred,
    mapping = aes(x = DE.DATE,
                  y = fit),
    colour = "firebrick3",
    size = 1
  ) +
  scale_size(range = c(0.5, 3)) +
  coord_cartesian(ylim = c(-0.5, 0.2),
                  expand = FALSE) +
  xlab("Date") + ylab(expr(paste("Drift rate (ms"^"-1",")"))) +
  # ggtitle(paste0(sdive$ID[1], " (",sdive$SPECIES.X[1],", ",sdive$SEX.X,", ",sdive$AGECLASS.X,")")) +
  theme_bw()
# g1
# Simple track map
xlim = range(sdive$LON.X, na.rm=T)
ylim = range(sdive$LAT.X, na.rm=T)

s_track <- sdive %>% 
  mutate(DATE = substr(DATE,1,10)) %>% 
  group_by(DATE) %>% 
  summarise(lon = mean(LON.X),
            lat = mean(LAT.Y)
            )
d_track <- tstFit$pred %>% 
  mutate(DATE = substr(DE.DATE,1,10))

plot.d.track <- left_join(s_track, d_track, by = "DATE") %>% 
  mutate(d_drate = fit - lag(fit))

g_track <- ggplot(data = plot.d.track,
                  mapping = aes(x = lon,
                                y = lat,
                                colour = d_drate
                  )
) +
  geom_polygon(mapping = aes(x = long,
                             y = lat,
                             group = group),
               data = wrld,
               fill = "grey60",
               colour = NA) +
  geom_path(#colour = "#542788",
            size = 1) + 
  scale_colour_cmocean(name = "balance",
                       guide = guide_colourbar(
                         title = expr(paste("Daily drift rate change (m.s"^"-1",")")),
                         direction = "vertical",
                         
                         barwidth = 0.5,
                         barheight = 11,
                         title.position = "left",
                         title.hjust = 0.5
                         
                       )) +
  coord_fixed(xlim = xlim,
              ylim = ylim,
              expand = TRUE,
              ratio = 2
  ) +
  theme(
    legend.title = element_text(angle = 90),
    panel.background = element_blank(),
    panel.grid.major = element_line(colour = "grey"),
    panel.border = element_rect(fill = NA,
                                colour = "grey")
  )
# g_track

plots <- grid.arrange(g_track, g1, ncol = 1,
                      top = paste0(sdive$ID[1], " (",sdive$SPECIES.X[1],", ",sdive$SEX.X,", ",sdive$AGECLASS.X,")")
                      )
ggsave(filename = paste0("./data_qc_plots/d-rate_qc_plots","/", sdive$ID[1],"_drift-rate_qc.pdf"),
       plot = plots,
       device = "pdf",
       height = 29.7,
       width = 21,
       units = "cm"
)
}
