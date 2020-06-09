## Packages ####

listOfPackages <- c("tidyverse", "dbplyr", "DBI", "odbc", "janitor", "lubridate", "magrittr", "devtools")

function_load <- function(pkgs) {
  for (pkg in pkgs) {
    if (!require(pkg, character.only = TRUE)) {
      install.packages(pkg, dependencies = TRUE)
      require(pkg, character.only = TRUE)
    }
  }
}

function_load(listOfPackages)

## Establish Connection ####

con <-
  dbConnect(odbc(), Driver = "SQL Server", server = "MLCSU-BI-SQL-SU")

## Source function code ####

source_url("https://raw.githubusercontent.com/The-Strategy-Unit/covid_outpatients_pulling/master/internal_funtion_code.R")

## Parameters ####

Provider_Code <- "RL4"
Provider_Code_00 <- "RL400"
Specialty <- 110
Specialty_name <- "Trauma and orthopaedic surgery"
Final_Date <- "2020-06-30" ## as YY-MM-DD

## Use function ####

outpatients <- Pull_From_SQL(Provider_Code = Provider_Code,
                             Provider_Code_00 = Provider_Code_00,
                      Specialty = Specialty,
                      Treatment_Code = paste0("C_", Specialty),
                      Specialty_name = Specialty_name, 
                      Final_Date = Final_Date)

## See full script inside RStudio ####

outpatients[["Binded"]] %>% view

## Write to CSV ####

write_csv(outpatients[["Binded"]], paste0(Provider_Code, "_", str_replace_all(Specialty_name, " ", "_"), ".csv"), na = '')

## Save ####

save.image("workspace.RData")

###############################
## EXTRA INFORMATION BELOW ####
###############################


## Look up for relevant dates ####

dates_lookup <- tibble(day = seq.Date(as.Date("2018-01-01"),
                                      as.Date("2020-12-31"),
                                      by = "day"),
                       week = week(day),
                       formattedweek = day %>% map_chr(function(x) {
                         paste0(year(x), "-", if (str_count(week(x)) == 1) {
                           paste0(0, week(x))
                         } else {
                           week(x)
                         }
                         )
                       }),
                       month = month(day)
)
## Available Specialties ####

# https://www.datadictionary.nhs.uk/web_site_content/supporting_information/main_specialty_and_treatment_function_codes_table.asp

available_specialties <- tibble(specialties = c("Emergency Medicine", "Geriatric medicine", "Haematology", 
  "Obstetrics and Gynaecology", "Ophthalmology", "Otolaryngology", 
  "Trauma and orthopaedic surgery", "Anaesthetics", "Cardiology", 
  "Cardio-thoracic surgery", "Chemical pathology", "Clinical oncology", 
  "Clinical radiology", "Dermatology", "Endocrinology and diabetes mellitus", 
  "Gastroenterology", "General (internal) medicine", "General surgery", 
  "Genito-urinary medicine", "Histopathology", "Medical microbiology", 
  "Medical oncology", "Neurology", "Oral and maxillo-facial surgery", 
  "Orthodontics", "Paediatrics", "Palliative medicine", "Rehabilitation medicine", 
  "Renal medicine", "Respiratory medicine", "Rheumatology", "Urology", 
  "Vascular Surgery", "Acute Internal Medicine", "General psychiatry", 
  "General Practice (GP) 6 month Training", "Public Health Medicine", 
  "General Dental Practitioner", "Clinical neurophysiology", "General Med Practitioner", 
  "Immunology", "Oral and Maxillofacial Surgery", "Gastro-enterology", 
  "Oral Surgery"))

## Available Specialty Codes ####

available_specialty_codes <- structure(list(Treatment_Function_Code = c("X01", "C_300", "C_502", 
                                           "C_120", "C_340", "C_410", "C_999", "C_100", "C_301", "C_330", 
                                           "C_430", "C_160", "C_140", "C_101", "C_170", "C_400", "C_320", 
                                           "C_130", "C_110")), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, 
                                                                                                                       -19L))

