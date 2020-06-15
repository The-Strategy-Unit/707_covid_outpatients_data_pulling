Data_check <- tibble(
  Specialty = c(100, 101, 110, 120, 130, 140, 150, 160, 170, 300, 301, 320, 330, 340, 400, 410, 430, 502)
  ,
  # Specialty = c("General surgery", "Urology", "Trauma and orthopaedic surgery", "Otolaryngology", "Ophthalmology", "Oral Surgery", "Neurosurgery", "Plastic surgery", "Cardio-thoracic surgery", "General (internal) medicine", "Gastroenterology", "Cardiology", "Dermatology", "Respiratory medicine", "Neurology", "Rheumatology", "Geriatric medicine", "Obstetrics and Gynaecology")
  # ,
  # Data = "Theatres",
                     RTG = NA,
                     RWE = NA,
                     RR1 = NA,
                     RJE = NA,
                     RL4 = NA,
                     RXK = NA,
                     RRK = NA,
                     RWP = NA,
                     RNA = NA,
                     RXW = NA,
                     RQ3 = NA,
                     RLQ = NA,
                     RBK = NA,
                     RL1 = NA,
                     RKB = NA,
                     RYW = NA,
                     RRJ = NA,
                     RRE = NA)

Data_check_list <- list()
Data_check_list[["Referrals"]] <- moo
Data_check_list[["Occupied_Beds_Daycare"]] <- Data_check
Data_check_list[["Occupied_Beds_Overnight"]] <- Data_check
Data_check_list[["Staffing"]] <- Data_check
Data_check_list[["GP_Referrals"]] <- Data_check
Data_check_list[["NotSeen_Recovered"]] <- Data_check
Data_check_list[["NotSeen_Died"]] <- Data_check
Data_check_list[["Theatres"]] <- Data_check
Data_check_list[["Sickness"]] <- Data_check

for (trust in colnames(Data_check)[-1]) {
  test <- list()
  # for (specialty in Data_check$Specialty) {
    test[[paste(specialty)]] <- tbl(
      con,
      build_sql(
        "SELECT COUNT(*) AS NROWS FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Effective_Snapshot_Date
		, No_of_Operating_Theatres
	FROM [central_midlands_csu_UserDB].[Cancelled_Elec_Ops].[Supporting_Facilities1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'') 
	AND Organisation_Code = '", Provider_Code, "'", "
      '
      )",
        con = con
      )
    ) %>% collect()
    
    
    if (test[[paste(specialty)]] %>% pull(NROW) > 0) {
      Data_check_list[["Theatres"]][[1,trust]] <- TRUE
    } else if (test[[paste(specialty)]] %>% pull(NROW) == 0) {
      Data_check_list[["Theatres"]][[1,trust]] <- FALSE
    }
    
    cat(trust, specialty, "finished\n")
}

####

for (trust in colnames(Data_check)[-1]) {

  # for (specialty in Data_check$Specialty) {
  foobar <- tbl(
    con,
    build_sql(
      "SELECT COUNT(*) AS NROWS FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Organisation_Type
		, FTE_Days_Sick
		, FTE_Days_Available
		, Effective_Snapshot_Date
FROM [central_midlands_csu_UserDB].[NHS_Workforce].[Sickness_Absence1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'') ",
      "AND Organisation_Code = '",
      trust,
      "' ",
      "' )",
      con = con
    )
  ) %>% collect()
  
  
  if (foobar %>% pull(NROWS) > 0) {
    Data_check_list[["Sickness"]][1,trust] <- TRUE
  } else if (foobar %>% pull(NROWS) == 0) {
    Data_check_list[["Sickness"]][1,trust] <- FALSE
  }
  
  cat(trust, specialty, "finished\n")
}

####


openxlsx::write.xlsx(Data_check_list, "Datacheck.xlsx")
