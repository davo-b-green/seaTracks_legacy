
# ----------------------------------------------------------------------------------------------------------------------------------- #
library(curl)
library(dplyr)
library(RODBC)
library(fs)
library(stringr)
library(tidyselect)
library(readr)
library(here)
# ----------------------------------------------------------------------------------------------------------------------------------- #

# Get relative file paths for 

p <- "c:/Users/greendb/OneDrive - University of Tasmania/SMRU Data/" # directory with zipped files
df <- "ACCESS_Files" # Destination for unzipped files

# ----------------------------------------------------------------------------------------------------------------------------------- #

# Unzipping files #

## Get list of all *.zip files
flz <- dir_ls(path = p,
             glob = "*.zip")

## Unzip files to new directory
lapply(flz, function(x){
  unzip(zipfile = x,
        exdir = paste0(p, df, "/"),
        overwrite = FALSE)
})

# ----------------------------------------------------------------------------------------------------------------------------------- #
# Combining datasets #

## Get list of all ACCESS files
fl <- dir_ls(path = paste0(p, df, "/"),
             glob = "*.mdb")


# ----------------------------------------------------------------------------------------------------------------------------------- #
## Consolidate location data files

dataLoc <- lapply(fl, function(x){
  cat("Processing locations: ", x, "\n")
  con = odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=",x))
  data = sqlFetch(con, "Diag")
  data = select(data, -which(as.character(sapply(data, class)) %in% "ODBC_binary")) |>  # remove unrecognized ODBC_binary variables
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
    ) |> 
    rename_with(.cols = everything(), .fn = toupper) # Change all colnames to uppercase

  odbcClose(con) # close ACCESS db connection
  return(data)
}) |> 
  bind_rows()


# ----------------------------------------------------------------------------------------------------------------------------------- #
## Create dive dataset
dataDive <- lapply(fl, function(x){
  cat("Processing dives: ", x, "\n")
  con = odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=",x))
  
  data = tryCatch( # Catch any errors coming from their being no dive data
    sqlFetch(con, "Dive"),
    error = function(e){
      message(paste("Dive data do not exist for", str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
    })
  
if(is.null(data)){
  return(data.frame(REF = str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
  odbcClose(con) # close ACCESS db connection
}else{  
  data = select(data, -which(as.character(sapply(data, class)) %in% "ODBC_binary")) |>  # remove unrecognised ODBC_binary variables
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
    ) |> 
    rename_with(.cols = everything(), .fn = toupper) # Change all colnames to uppercase
  return(data)
  odbcClose(con) # close ACCESS db connection
}
}) |>  
  bind_rows()
# ----------------------------------------------------------------------------------------------------------------------------------- #