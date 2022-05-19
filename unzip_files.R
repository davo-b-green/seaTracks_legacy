# Title: Code for unzipping SRDL files and saving associated ACCESS databases to "access_files" directory
# Compiled by David Green
# Date: 17-05-2022

# ----------------------------------------------------------------------------------------------------------------------------------- #
library(tidyverse)
library(fs)
library(here) # sets root directory
# ----------------------------------------------------------------------------------------------------------------------------------- #

# Get relative file paths
df <- here("access_files")

# ----------------------------------------------------------------------------------------------------------------------------------- #

# Unzipping files #

## Get list of all *.zip files
flz <- dir_ls(path = here(),
              glob = "*.zip")

## Unzip files to new directory
lapply(flz, function(x){
  unzip(zipfile = x,
        exdir = df,
        overwrite = FALSE)
})
