# Title: Code for extracting location and dive data from associated ACCESS databases, and compiling 
# into master location and dive datasets
# Compiled by David Green
# Date: 17-05-2022

# ----------------------------------------------------------------------------------------------------------------------------------- #
library(curl)
library(RODBC)
library(tidyverse)
library(fs)
library(here) # sets root directory
# ----------------------------------------------------------------------------------------------------------------------------------- #

dir_create(path = here("compiled_datasets")) # Create directory for the compiled loc and dive datasets

# ----------------------------------------------------------------------------------------------------------------------------------- #
# Combining datasets #

## Get list of all ACCESS files
fl <- dir_ls(path = here("access_files"),
             glob = "*.mdb")


# ----------------------------------------------------------------------------------------------------------------------------------- #
## Consolidate location data files

dataLoc <- lapply(fl, function(x){
  cat("Processing locations: ", x, "\n")
  con = odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=",x))
  data = sqlFetch(con, 
                  "diag", 
                  as.is = TRUE
                  )
  data = select(data, -which(as.character(sapply(data, class)) %in% "ODBC_binary")) %>%  # remove unrecognized ODBC_binary variables
     mutate( 
      across( # Correcting decimal degrees that have comma separator
        where(~ is.character(.x) && 
                isTRUE(
                  max(str_count(.x, ","), na.rm = TRUE) == 1 &&
                    any(str_count(.x, ",")) == 1
                  )
          ),
        ~ as.numeric(str_replace(.x, ",", "."))
      ),
      across( # Coercing everything to character prior to binding
        .cols = everything(),
        .fns = as.character
        )
    ) %>% 
    rename_with(.cols = everything(), .fn = tolower) # Change all colnames to lowercase

  odbcClose(con) # close ACCESS db connection
  return(data)
}) %>% 
  bind_rows() %>% 
  rowwise() %>% 
  mutate(campaign = str_split(ref, pattern = "[\\_-]+")[[1]][1],
         d_date = as.POSIXct(d_date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))

# Save dataset
write_delim(x = dataLoc,
            file = here("compiled_datasets", "loc_all_raw_pre-qc.txt"),
            delim = "\t"
            )

# ----------------------------------------------------------------------------------------------------------------------------------- #
## Create dive dataset
dataDive <- lapply(fl, function(x){
  cat("Processing dives: ", x, "\n")
  con = odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=",x))
  
  data = tryCatch( # Catch any errors coming from their being no dive data
    sqlFetch(con, "dive", as.is = TRUE
    ),
    error = function(e){ 
      message(paste("Unable to find dive data for", str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
    })
  
if(is.null(data)){
  odbcClose(con) # close ACCESS db connection
  return(data.frame(REF = str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
}else{  
  data = select(data, -which(as.character(sapply(data, class)) %in% "ODBC_binary")) %>%  # remove unrecognised ODBC_binary variables
    mutate( 
      across( # Correcting decimal degrees that have comma separator
        where(~ is.character(.x) && 
                isTRUE(
                  max(str_count(.x, ","), na.rm = TRUE) == 1 &&
                    any(str_count(.x, ",")) == 1
                )
        ),
        ~ as.numeric(str_replace(.x, ",", "."))
      ),
      across( # Coercing everything to character prior to binding
        .cols = everything(),
        .fns = as.character
      )
    ) %>% 
    rename_with(.cols = everything(), .fn = tolower) # Change all colnames to lowercase
  odbcClose(con) # close ACCESS db connection
  return(data)
  
}
}) %>%  
  bind_rows() %>% 
  rowwise() %>% 
  mutate(campaign = str_split(ref, pattern = "[\\_-]+")[[1]][1],
         de_date = as.POSIXct(de_date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))

# Save dataset
write_delim(x = dataDive,
            file = here("compiled_datasets", "dive_all_raw_pre-qc.txt"),
            delim = "\t"
)
# ----------------------------------------------------------------------------------------------------------------------------------- #