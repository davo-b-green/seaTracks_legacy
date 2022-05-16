
# ----------------------------------------------------------------------------------------------------------------------------------- #
library(curl)
library(dplyr)
library(RODBC)
library(fs)
library(stringr)
library(tidyselect)

# ----------------------------------------------------------------------------------------------------------------------------------- #

# Get relative file paths for 
here::i_am(path = "./Consolidating_SES_tracking_datasets.R") # set root directory for project
# p <- "c:/Users/greendb/OneDrive - University of Tasmania/SMRU Data/" # directory with zipped files
p <- here::here("")
# df <- "ACCESS_Files" # Destination for unzipped files
df <- here::here("ACCESS_Files")
fs::dir_create(path = here::here("Consolidated_datasets"))

# ----------------------------------------------------------------------------------------------------------------------------------- #

# Unzipping files #

## Get list of all *.zip files
flz <- dir_ls(path = p,
             glob = "*.zip")

## Unzip files to new directory
lapply(flz, function(x){
  unzip(zipfile = x,
        exdir = df,
        overwrite = FALSE)
})

# ----------------------------------------------------------------------------------------------------------------------------------- #
# Combining datasets #

## Get list of all ACCESS files
fl <- dir_ls(path = df,
             glob = "*.mdb")


# ----------------------------------------------------------------------------------------------------------------------------------- #
## Consolidate location data files

dataLoc <- lapply(fl, function(x){
  cat("Processing locations: ", x, "\n")
  con = odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=",x))
  data = sqlFetch(con, "diag")
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

# Save dataset
write_delim(x = dataLoc,
            file = here::here("Consolidated_datasets", "loc_all_ses_raw_pre-qc.txt"),
            delim = "\t"
            )

# ----------------------------------------------------------------------------------------------------------------------------------- #
## Create dive dataset
dataDive <- lapply(fl, function(x){
  cat("Processing dives: ", x, "\n")
  con = odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=",x))
  
  data = tryCatch( # Catch any errors coming from their being no dive data
    sqlFetch(con, "dive"),
    error = function(e){
      message(paste("Unable to find dive data for", str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
    })
  
if(is.null(data)){
  odbcClose(con) # close ACCESS db connection
  return(data.frame(REF = str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
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
  odbcClose(con) # close ACCESS db connection
  return(data)
  
}
}) |>  
  bind_rows()

# Save dataset
write_delim(x = dataDive,
            file = here::here("Consolidated_datasets", "dive_all_ses_raw_pre-qc.txt"),
            delim = "\t"
)
# ----------------------------------------------------------------------------------------------------------------------------------- #