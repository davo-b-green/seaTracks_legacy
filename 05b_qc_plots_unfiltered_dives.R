library(tidyverse)
library(quantreg)
library(data.table)
library(here)
library(fs)
library(future.apply)

dir_create("./data_qc_plots/dive-qc-plots/")

dive <- fread(here("compiled_datasets/dive_all_raw_pre-qc.txt")) %>%  
  rename(id = ref,
         date = de_date
  ) 

idmeta <- fread(file = "IMOS_CTD_metadata.csv")
idmeta <- idmeta %>%
  dplyr::rename(id=SMRU_Ref)

dive <- left_join(dive, idmeta, by = "id")

### Create qc plots of dive_dur~max_dep for individual deployments
qc_dat <- dive %>% 
  select(id, max_dep, dive_dur, date, d_mask, Species, Sex, AgeClass)

plan("future::multisession")

# lapply(unique(qc_dat$id), function(x){
future_lapply(unique(qc_dat$id), function(x){
  subDat = subset(qc_dat, id %in% x) %>% 
    na.omit() %>% 
    mutate(qc_flag = case_when(d_mask == 1 ~ "Yes",
                               d_mask == 0 ~ "No")
           )
    
  g = ggplot(subDat, 
             mapping = aes(x = max_dep, 
                           y = dive_dur,
                           colour = qc_flag)
             ) +
    geom_point() +
    scale_colour_manual(values = c("grey30", "indianred2")) +
    coord_cartesian(
      # xlim = xlim,
      # ylim = ylim,
      expand = FALSE
    ) +
    xlab("max dep (m)") +
    ylab("dive dur (s)") +
    ggtitle(paste0(x, " (",subDat$Species[1],", ",subDat$Sex,", ",subDat$AgeClass,")")) +
    theme_bw()
  ggsave(filename = paste0("./data_qc_plots/dive-qc-plots/",x,"_dive-qc.png"),
         plot = g)
})
plan(sequential)
