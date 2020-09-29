################
## Packages ####
################

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

############################
## Establish Connection ####
############################

con <-
  dbConnect(odbc(), Driver = "SQL Server", server = "MLCSU-BI-SQL-SU")

## Source function code ####

source_url("https://raw.githubusercontent.com/The-Strategy-Unit/707_covid_outpatients_data_pulling/master/regional_function_code.R")

##################
## Parameters ####
##################

RegionCode <- "Y56" ## Y56, Y58-Y63 
Specialty <- "110"
Specialty_name <- "Trauma and orthopaedic surgery"
Treatment_Code <- if (Specialty == "X01") "X01" else paste0("C_", Specialty) ## don't need to change this line
Final_Date <- "2021-04-05" ## as YY-MM-DD

## Note: If using X01 specify "Other" as Specialty_name

####################
## Use function ####
####################

outpatients <- Pull_From_SQL_Regional(RegionCode = RegionCode,
                      Specialty = Specialty,
                      Treatment_Code = Treatment_Code,
                      Specialty_name = Specialty_name, 
                      Final_Date = Final_Date)

##############################################
## See finished data frame inside RStudio ####
##############################################

outpatients %>% view

## Write to CSV ####

write_csv(outpatients, paste0(RegionCode, "_", str_replace_all(Specialty_name, " ", "_"), ".csv"), na = '')

############
## Save ####
############

save.image("workspace.RData")

###############################
## EXTRA INFORMATION BELOW ####
###############################

## Available Specialties ####

# https://www.datadictionary.nhs.uk/web_site_content/supporting_information/main_specialty_and_treatment_function_codes_table.asp

available_specialties <-
  structure(
    list(
      Specialty = c(
        "Acute Internal Medicine",
        "Additional dental specialties",
        "Allergy",
        "Anaesthetics",
        "Audio Vestibular Medicine",
        "Cardio-thoracic surgery",
        "Cardiology",
        "Chemical pathology",
        "Child and adolescent psychiatry",
        "Clinical genetics",
        "Clinical neurophysiology",
        "Clinical oncology",
        "Clinical pharmacology and therapeutics",
        "Clinical radiology",
        "Community Health Service Dental",
        "Community Health Service Medical",
        "Community Sexual and Reproductive Health",
        "Dental and Maxillofacial Radiology",
        "Dental Public Health",
        "Dermatology",
        "Diagnostic Neuropathology",
        "Emergency Medicine",
        "Endocrinology and diabetes mellitus",
        "Endodontics",
        "Forensic Histopathology",
        "Forensic psychiatry",
        "Gastro-enterology",
        "Gastroenterology",
        "General (internal) medicine",
        "General Dental Practitioner",
        "General Med Practitioner",
        "General pathology",
        "General Practice (GP) 6 month Training",
        "General psychiatry",
        "General surgery",
        "Genito-urinary medicine",
        "Geriatric medicine",
        "Haematology",
        "Histopathology",
        "Immunology",
        "Infectious diseases",
        "Intensive care medicine",
        "Medical microbiology",
        "Medical oncology",
        "Medical ophthalmology",
        "Neurology",
        "Neurosurgery",
        "Nuclear medicine",
        "Obstetrics and Gynaecology",
        "Occupational medicine",
        "Old age psychiatry",
        "Ophthalmology",
        "Oral and maxillo-facial surgery",
        "Oral and Maxillofacial Pathology",
        "Oral and Maxillofacial Surgery",
        "Oral Medicine",
        "Oral Surgery",
        "Orthodontics",
        "Other",
        "Other Specialties",
        "Otolaryngology",
        "Paediatric and Perinatal Pathology",
        "Paediatric cardiology",
        "Paediatric dentistry",
        "Paediatric surgery",
        "Paediatrics",
        "Palliative medicine",
        "Periodontics",
        "Plastic surgery",
        "Prosthodontics",
        "Psychiatry of learning disability",
        "Psychotherapy",
        "Public Health Medicine",
        "Rehabilitation medicine",
        "Renal medicine",
        "Respiratory medicine",
        "Restorative dentistry",
        "Rheumatology",
        "Special Care Dentistry",
        "Sport and Exercise Medicine",
        "Surgical Dentistry",
        "Trauma and orthopaedic surgery",
        "Tropical medicine",
        "Urology",
        "Vascular Surgery",
        "Virology"
      )
    ),
    class = c("tbl_df", "tbl", "data.frame"),
    row.names = c(NA,-86L)
  )

## Available Specialty Codes ####

available_specialty_codes <- structure(list(Treatment_Function_Code = c("X01", "C_300", "C_502", 
                                           "C_120", "C_340", "C_410", "C_999", "C_100", "C_301", "C_330", 
                                           "C_430", "C_160", "C_140", "C_101", "C_170", "C_400", "C_320", 
                                           "C_130", "C_110")), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, 
                                                                                                                       -19L))

