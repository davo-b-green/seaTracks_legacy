library(tidyverse)

here::i_am("./xref_loc-dive-and-meta.R")

# Unique campaigns with location data
dataDiag <- read_delim(file = here::here("Consolidated_datasets/loc_all_ses_raw_pre-qc.txt"),
                       delim = "\t") |> 
  select(CAMPAIGN) |> 
  distinct() |> 
  mutate(db_diag = "y") |> 
  rename_with(.cols = everything(),
              .fn = tolower)

# unique campaigns with dive data
dataDive <- read_delim(file = here::here("Consolidated_datasets/dive_all_ses_raw_pre-qc.txt"),
                       delim = "\t") |> 
  select(CAMPAIGN) |> 
  distinct() |> 
  mutate(db_dive = "y") |> 
  rename_with(.cols = everything(),
              .fn = tolower)

# Cross-referencing against meta & available downloaded datasets
dMeta <- read_csv(file = "decade_meta_DG.csv") # decade meta data
meta <- readxl::read_excel(path = "IMOS_CTD_metadata_02022022_DG.xlsx") |> # IMOS meta
  rowwise() |> 
  mutate(campaign = str_split(ref, pattern = "[\\_-]+")[[1]][1]) |> 
    select(campaign) |> 
    distinct() |> 
    mutate(imos_meta = "y")

dMeta <- list(dMeta, dataDiag, dataDive, meta) |> # Corss-referencing decade metadata to see if we have corresponding location/dive data, and imos meta for each campaign
  reduce(left_join, by = "campaign") |> 
  mutate(
    across(
      .cols = everything(),
      .fns = ~ replace(., is.na(.), "n")
    )
  )
  

