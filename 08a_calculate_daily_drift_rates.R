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

dive <- dive %>% 
  select_if(!duplicated(toupper(names(.)))) %>% 
  rename_with(~ str_replace_all(.x, pattern = "\\_", replacement = ".")) %>% 
  rename_with(.cols = everything(), .fn = toupper) %>%  # Change all colnames to lowercase
  filter(D.MASK == 0) %>% 
  group_split(ID)

plan("future::multisession") 
dRates <- future_lapply(dive, function(z) {# Calculates daily drift rates using same methods as used in plots
  sdive <- rbs(z, num=NA)
  sdive <- pDrift(sdive)
  tstFit <- tryCatch(fitDrate(sdive),
                     error = function(e){ 
                       message(paste("cannot compute drift rates for: ", z$ID[1]))
                     })
  if(is.null(tstFit)){tstFit = head(sdive,0)}else{tstFit = tstFit$data}
  return(tstFit)
})
plan(sequential)
dRates <- bind_rows(dRates)

write_csv(dRates, file = "./processed_datasets/daily-drift-rates.csv")
