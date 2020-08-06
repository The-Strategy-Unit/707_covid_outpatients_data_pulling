################
## Packages ####
################

listOfPackages <- c("tidyverse", "dbplyr", "DBI", "odbc", "janitor", "lubridate", "magrittr", "devtools", "writexl")

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

############################
## Source function code ####
############################

source_url("https://raw.githubusercontent.com/The-Strategy-Unit/707_covid_outpatients_data_pulling/master/internal_function_code.R")

##################
## Parameters ####
##################

Provider_Code <- "RNS"
Provider_Code_00 <- "RNS00"
Final_Date <- "2021-04-05" ## as YY-MM-DD

####################
## Use function ####
####################

container_list <- list() ## WARNING: this resets the list object each time you want to change the provider

container_list <- map2((outpatients_tibble$codes %>% setNames(paste0(outpatients_tibble$codes, "-", outpatients_tibble$code_names))), outpatients_tibble$code_names, ~ {
  
cat(paste0("## Running ", Provider_Code, " ", .x, "-", .y, " ##\n"))

safe_pull <- safely(Pull_From_SQL, otherwise = tibble())

outpatients <- safe_pull(Provider_Code = Provider_Code,
                         Provider_Code_00 = Provider_Code_00,
                         Specialty = .x,
                         Treatment_Code = if (.x == "X01") "X01" else paste0("C_", .x),
                         Specialty_name = .y, 
                         Final_Date = Final_Date)

cat(paste0("## ", Provider_Code, " ", .x, "-", .y, " complete ##\n"))

return(outpatients)
}
)

##########################
## Write to XLSX file ####
##########################

writexl::write_xlsx(map(container_list, 1), paste0(Provider_Code, ".xlsx"))

############
## Save ####
############

save.image("workspace.RData")

