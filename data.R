library(icesTAF)
library(jsonlite)
library(icesConnect)
library(httr)
library(dplyr)

# load saved JWT
source("set_token.R")

# settings
config <- read_json("config.json", simplifyVector = TRUE)
config$VE_session_ID <- trimws(strsplit(as.character(config$VE_session_ID), ",")[[1]])
config$LE_session_ID <- trimws(strsplit(as.character(config$LE_session_ID), ",")[[1]])
# create directories
mkdir(config$data_dir)

# field name mappings -----------------------------------------------------

# VE: API field -> Rmd-expected field
# where a _txt variant exists in the API response, we take the _txt version
# (as numeric) and map it to the Rmd name, dropping the plain numeric version
ve_field_map <- c(
  sessionID               = "SessionID",
  parentLineNumber        = "ParentLineNumber",
  lineNumber              = "LineNumber",
  recordType              = "RecordType",
  countryCode             = "CountryCode",
  year                    = "Year",
  month_txt               = "Month",
  noDistinctVessels_txt   = "NoDistinctVessels",
  anonymizedVesselID      = "AnonymizedVesselID",
  csquare                 = "Csquare",
  metierL4                = "MetierL4",
  metierL5                = "MetierL5",
  metierL6                = "MetierL6",
  vesselLengthRange       = "VesselLengthRange",
  averageFishingSpeed_txt = "AverageFishingSpeed",
  fishingHour_txt         = "FishingHour",
  averageVesselLength_txt = "AverageVesselLength",
  averagekW_txt           = "AveragekW",
  kwFishingHour_txt       = "kWFishingHour",
  totWeight_txt           = "TotWeight",
  totValue_txt            = "TotValue",
  averageGearWidth        = "AverageGearWidth",
  averageInterval         = "AverageInterval",
  habitatType             = "HabitatType",
  depthRange              = "DepthRange",
  numberOfRecords         = "NumberOfRecords",
  sweptArea               = "SweptArea"
)

# LE: API field -> Rmd-expected field (no _txt variants here)
le_field_map <- c(
  sessionID          = "SessionID",
  lineNumber         = "LineNumber",
  recordType         = "RecordType",
  countryCode        = "CountryCode",
  year               = "Year",
  month              = "Month",
  noDistinctVessels  = "NoDistinctVessels",
  anonymizedVesselID = "AnonymizedVesselID",
  iceSrectangle      = "ICESrectangle",
  metierL4           = "MetierL4",
  metierL5           = "MetierL5",
  metierL6           = "MetierL6",
  vesselLengthRange  = "VesselLengthRange",
  vmsEnabled         = "VMSEnabled",
  fishingDays        = "FishingDays",
  kWFishingDays      = "kWFishingDays",
  totWeight          = "TotWeight",
  totValue           = "TotValue"
)

# helpers -----------------------------------------------------------------

fetch_session <- function(url) {
  resp <- ices_get_jwt(url)
  stop_for_status(resp)
  fromJSON(content(resp, as = "text", encoding = "UTF-8"))
}

# take a parsed data frame and a named mapping vector (api_name = rmd_name)
# returns a data frame with only the mapped columns, renamed, and _txt columns
# converted to numeric
apply_mapping <- function(df, mapping) {
  # which api fields do we want? (names of the mapping vector)
  keep <- intersect(names(mapping), names(df))
  df <- df[, keep, drop = FALSE]
  
  # convert any _txt columns to numeric
  txt_cols <- grep("_txt$", names(df), value = TRUE)
  for (col in txt_cols) {
    df[[col]] <- as.numeric(df[[col]])
  }
  
  # rename to rmd-expected names
  names(df) <- mapping[names(df)]
  df
}

# VMS (VE) ----------------------------------------------------------------

ve_list <- list()
for (sid in config$VE_session_ID) {
  msg("downloading VE data for session ... ", sid)
  url <- paste0("https://data.ices.dk/VMS/API/GetVMSSessionsData?SessionID=", sid)
  ve_list[[as.character(sid)]] <- fetch_session(url)
}
ve <- bind_rows(ve_list)
ve <- apply_mapping(ve, ve_field_map)

ve_fname <- paste0(config$data_dir, "/ICES_VE_", config$countries, ".csv")
write.csv(ve, file = ve_fname, row.names = FALSE, na = "")
msg("wrote ", nrow(ve), " VE rows to ", ve_fname)

# Logbook (LE) ------------------------------------------------------------

le_list <- list()
for (sid in config$LE_session_ID) {
  msg("downloading LE data for session ... ", sid)
  url <- paste0("https://data.ices.dk/VMS/API/GetLogbookSessionsData?SessionID=", sid)
  le_list[[as.character(sid)]] <- fetch_session(url)
}
le <- bind_rows(le_list)
le <- apply_mapping(le, le_field_map)

le_fname <- paste0(config$data_dir, "/ICES_LE_", config$countries, ".csv")
write.csv(le, file = le_fname, row.names = FALSE, na = "")
msg("wrote ", nrow(le), " LE rows to ", le_fname)
