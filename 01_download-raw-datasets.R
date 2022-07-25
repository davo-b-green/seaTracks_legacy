# Download tracking datasets from SMRU portal

# Author: David Green
# Date: 18 May 2022

library(tidyverse)
library(httr)
library(here)
library(fs)
library(data.table)

# Create directory for downloaded files
dir_create(path = "./downloaded_campaigns")

# Input usernames and passwords - currently only IMOS/CEBC/UTAS campaigns supported
{
un_imos <- readline("Input IMOS username (leave blank if unknown):")
pw_imos <- readline("Input IMOS password (leave blank if unknown):")

un_cebc <- readline("Input CEBC username (leave blank if unknown):")
pw_cebc <- readline("Input CEBC password (leave blank if unknown):")

un_utas <- readline("Input UTAS username (leave blank if unknown):")
pw_utas <- readline("Input UTAS password (leave blank if unknown):")
}

# Available campaigns
camps <- read_csv("available_campaigns.csv")

## Check for already processed campaigns
if(file_exists("./compiled_datasets/loc_all_raw_pre-qc.txt")){
  camp_comp <- fread("./compiled_datasets/loc_all_raw_pre-qc.txt") %>% 
    pull(campaign) %>% 
    unique()
}else{
  camp_comp <- NULL
  }

# Get only undownloaded campaigns
`%nin%` <- Negate(`%in%`)
camps <- filter(camps,
                campaign %nin% camp_comp)

# 

lapply(1:nrow(camps), function(ii){
  x = camps$campaign[ii]
  prov = camps$provider[ii]
  un = case_when(prov %in% "imos" ~ un_imos,
                 prov %in% "cnrs" ~ un_cebc,
                 prov %in% "utas" ~ un_imos,
                 prov %in% "sou" ~ un_cebc
                 )
  
  pw = case_when(prov %in% "imos" ~ pw_imos,
                 prov %in% "cnrs" ~ pw_cebc,
                 prov %in% "utas" ~ pw_imos,
                 prov %in% "sou" ~ pw_cebc
                 )
  cat(paste("Downloading campaign: ", x, "\n"))
  tryCatch( # Catch any errors coming from file already having been downloaded, or errors with username/password
    GET(paste0("http://www.smru.st-andrews.ac.uk/protected/",x,"/db/",x,".zip"), 
        authenticate(un, pw), 
        write_disk(paste0(here(),"/downloaded_campaigns/",x,".zip")), 
        timeout(60)),
    error = function(e){
      message(paste("File exists or incorrect username/password - campaign:", x))
  })
})  
  
