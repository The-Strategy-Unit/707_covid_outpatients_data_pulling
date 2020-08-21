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

source_url("https://raw.githubusercontent.com/The-Strategy-Unit/707_covid_outpatients_data_pulling/master/national_function_code.R")

####################
## Use function ####
####################

container_list <- list() ## WARNING: this resets the list object each time you want to change the provider

container_list <- map2((outpatients_tibble$codes %>% setNames(paste0(outpatients_tibble$codes, "-", outpatients_tibble$code_names)))[1:18], outpatients_tibble$code_names[1:18], ~ {
  
cat(paste0("## Running National ", .x, "-", .y, " ##\n"))

safe_pull <- safely(Pull_From_SQL_National, otherwise = tibble(a = NA))

outpatients <- safe_pull(Specialty = .x,
                         Treatment_Code = if (.x == "X01") "X01" else paste0("C_", .x),
                         Specialty_name = .y, 
                         Final_Date = "2021-04-05")

cat(paste0("## National ", .x, "-", .y, " complete ##\n"))

return(outpatients)
}
)

##########################
## Write to XLSX file ####
##########################

writexl::write_xlsx(map(container_list, 1), "National.xlsx")

############
## Save ####
############

save.image("workspace.RData")

