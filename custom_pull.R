## Packages ####

listOfPackages <- c("tidyverse","dbplyr","DBI","obdc","janitor",
                    "lubridate","magrittr")

for (i in listOfPackages){
  if(! i %in% installed.packages()){
    install.packages(i, dependencies = TRUE)
  }
  require(i)
}

## Establish Connection ####

con <-
  dbConnect(odbc(), Driver = "SQL Server", server = "MLCSU-BI-SQL-SU")

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

## Function Code ####

Pull_From_SQL <- function(Provider_Code, Specialty, Treatment_Code, Specialty_name) {
  
  ## Time ####
  
  start <- Sys.time()
  
  ## Create Empty List Container ####
  
  Provider_List <- list()
  
  ## Theatres Quarterly Data ####
  
  Provider_List[["Theatres"]] <- tbl(
    con,
    build_sql(
      "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Effective_Snapshot_Date
		, No_of_Operating_Theatres
		, No_of_Operating_Theatres_Day_Case_Only
	FROM [central_midlands_csu_UserDB].[Cancelled_Elec_Ops].[Supporting_Facilities1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'') 
	AND Organisation_Code = '", Provider_Code, "'", "
      '
      )",
      con = con
    )
  ) %>% collect()
  
  cat("1/7) Theatres pulled\n")
  
  ## Cancelled Elective Operations ####
  
  # Provider_List[["Cancelled_Elective_Ops"]] <- tbl(
  #   con, 
  #   sql(
  #     "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT 
  #      Provider_Code AS Organisation_Code
  # 		,Last_Minute_Cancelled_Ops_Non_Clinical_Reason
  # 		,Number_Of_Cancellations_On_The_Day_Of_Surgery
  # 		,Patients_Not_Treated_Within_X_Of_Cancellation
  # 		,X
  # 		,Effective_Snapshot_Date
  #  FROM [central_midlands_csu_UserDB].[Cancelled_Elec_Ops].[Cancelled_Elective_Ops_By_Provider1]#
  # 	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  #   )
  #   ) %>% filter(Organisation_Code == Provider_Code) %>% collect()
  
  ## X RTT Incomplete Monthly (in full) ####
  
  # Provider_List[["RTT_Incomplete"]] <- tbl(
  #   con,
  #   sql("SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT  Organisation_Code
  # 		, Treatment_Function_Code
  # 		, Number_Of_Weeks_Since_Referral
  # 		, Number_Of_Incomplete_Pathways
  # 		, Number_Of_Incomplete_Pathways_with_DTA
  # 		, Effective_Snapshot_Date
  # 	FROM [central_midlands_csu_UserDB].[RTT].[Incomplete_Pathways_Provider1]
  # 	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )")
  # ) %>% filter(Organisation_Code == Provider_Code, Treatment_Function_Code == 100) %>% collect()
  # 
  # Provider_List[["RTT_Incomplete_100"]] <- Provider_List[["RTT_Incomplete"]] %>% filter(Treatment_Function_Code == 100)
  
  ## X RTT Complete NonAdmitted Monthly (in full) ####
  
  # Provider_List[["RTT_Complete_NonAdm"]] <- tbl(
  #   con,
  #   sql(
  #     "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
  # 		, Treatment_Function_Code
  # 		, Number_Of_Weeks_Since_Referral
  # 		, Number_Of_Completed_NonAdmitted_Pathways
  # 		, Effective_Snapshot_Date
  # 	FROM [central_midlands_csu_UserDB].[RTT].[Completed_NonAdmitted_Pathways_Provider1]
  # 	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  #   )
  # ) %>% filter(Organisation_Code == Provider_Code) %>% collect()
  
  ## X RTT Completed Admitted Monthly (in full) ####
  
  # Provider_List[["RTT_Complete_Adm"]] <- tbl(
  #   con,
  #   sql(
  #     "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
  # 		, Treatment_Function_Code
  # 		, Number_Of_Weeks_Since_Referral
  # 		, Number_Of_Completed_Admitted_Pathways
  # 		, Effective_Snapshot_Date
  # 	FROM [central_midlands_csu_UserDB].[RTT].[Completed_Admitted_Pathways_Provider1]
  # 	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  #   )
  # ) %>% filter(Organisation_Code == Provider_Code) %>% collect()
  
  ## X RTT Referrals ####
  
  # Provider_List[["RTT_Referrals"]] <- tbl(
  #   con,
  #   sql(
  #     "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
  # 		, Treatment_Function_Code
  # 		, Number_Of_Incomplete_Pathways
  # 		, Number_Of_Incomplete_Pathways_With_DTA_For_Treatment
  # 		, Number_Of_New_RTT_Clock_Starts_In_Month	Report_Period_Length
  # 		, Effective_Snapshot_Date
  # 	FROM [central_midlands_csu_UserDB].[RTT].[New_Data_Items_Provider1]
  # 	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  #   )
  # ) %>% filter(Organisation_Code == Provider_Code, Treatment_Function_Code == "110") %>% collect()
  # Provider_List[["RTT_Referrals"]] <- Provider_List[["RTT_Referrals"]] %>% select(Effective_Snapshot_Date, Organisation_Code, Report_Period_Length) %>% rename(Num_RTT_Referrals = Report_Period_Length)
  
  ## RTT Full Table ####
  
  Provider_List[["RTT_Table"]] <- tbl(
    con,
    build_sql(
      "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Provider_Org_Code
, Commissioner_Org_Code
, RTT_Part_Name
, RTT_Part_Description
, Treatment_Function_Code
, Isnull(Gt_00_To_01_Weeks,0) AS Gt_00_To_01_Weeks
, Isnull(Gt_01_To_02_Weeks,0) AS Gt_01_To_02_Weeks
, Isnull(Gt_02_To_03_Weeks,0) AS Gt_02_To_03_Weeks
, Isnull(Gt_03_To_04_Weeks,0) AS Gt_03_To_04_Weeks
, Isnull(Gt_04_To_05_Weeks,0) AS Gt_04_To_05_Weeks
, Isnull(Gt_05_To_06_Weeks,0) AS Gt_05_To_06_Weeks
, Isnull(Gt_06_To_07_Weeks,0) AS Gt_06_To_07_Weeks
, Isnull(Gt_07_To_08_Weeks,0) AS Gt_07_To_08_Weeks
, Isnull(Gt_08_To_09_Weeks,0) AS Gt_08_To_09_Weeks
, Isnull(Gt_09_To_10_Weeks,0) AS Gt_09_To_10_Weeks
, Isnull(Gt_10_To_11_Weeks,0) AS Gt_10_To_11_Weeks
, Isnull(Gt_11_To_12_Weeks,0) AS Gt_11_To_12_Weeks
, Isnull(Gt_12_To_13_Weeks,0) AS Gt_12_To_13_Weeks
, Isnull(Gt_13_To_14_Weeks,0) AS Gt_13_To_14_Weeks
, Isnull(Gt_14_To_15_Weeks,0) AS Gt_14_To_15_Weeks
, Isnull(Gt_15_To_16_Weeks,0) AS Gt_15_To_16_Weeks
, Isnull(Gt_16_To_17_Weeks,0) AS Gt_16_To_17_Weeks
, Isnull(Gt_17_To_18_Weeks,0) AS Gt_17_To_18_Weeks
, Isnull(Gt_18_To_19_Weeks,0) AS Gt_18_To_19_Weeks
, Isnull(Gt_19_To_20_Weeks,0) AS Gt_19_To_20_Weeks
, Isnull(Gt_20_To_21_Weeks,0) AS Gt_20_To_21_Weeks
, Isnull(Gt_21_To_22_Weeks,0) AS Gt_21_To_22_Weeks
, Isnull(Gt_22_To_23_Weeks,0) AS Gt_22_To_23_Weeks
, Isnull(Gt_23_To_24_Weeks,0) AS Gt_23_To_24_Weeks
, Isnull(Gt_24_To_25_Weeks,0) AS Gt_24_To_25_Weeks
, Isnull(Gt_25_To_26_Weeks,0) AS Gt_25_To_26_Weeks
, Isnull(Gt_26_To_27_Weeks,0) AS Gt_26_To_27_Weeks
, Isnull(Gt_27_To_28_Weeks,0) AS Gt_27_To_28_Weeks
, Isnull(Gt_28_To_29_Weeks,0) AS Gt_28_To_29_Weeks
, Isnull(Gt_29_To_30_Weeks,0) AS Gt_29_To_30_Weeks
, Isnull(Gt_30_To_31_Weeks,0) AS Gt_30_To_31_Weeks
, Isnull(Gt_31_To_32_Weeks,0) AS Gt_31_To_32_Weeks
, Isnull(Gt_32_To_33_Weeks,0) AS Gt_32_To_33_Weeks
, Isnull(Gt_33_To_34_Weeks,0) AS Gt_33_To_34_Weeks
, Isnull(Gt_34_To_35_Weeks,0) AS Gt_34_To_35_Weeks
, Isnull(Gt_35_To_36_Weeks,0) AS Gt_35_To_36_Weeks
, Isnull(Gt_36_To_37_Weeks,0) AS Gt_36_To_37_Weeks
, Isnull(Gt_37_To_38_Weeks,0) AS Gt_37_To_38_Weeks
, Isnull(Gt_38_To_39_Weeks,0) AS Gt_38_To_39_Weeks
, Isnull(Gt_39_To_40_Weeks,0) AS Gt_39_To_40_Weeks
, Isnull(Gt_40_To_41_Weeks,0) AS Gt_40_To_41_Weeks
, Isnull(Gt_41_To_42_Weeks,0) AS Gt_41_To_42_Weeks
, Isnull(Gt_42_To_43_Weeks,0) AS Gt_42_To_43_Weeks
, Isnull(Gt_43_To_44_Weeks,0) AS Gt_43_To_44_Weeks
, Isnull(Gt_44_To_45_Weeks,0) AS Gt_44_To_45_Weeks
, Isnull(Gt_45_To_46_Weeks,0) AS Gt_45_To_46_Weeks
, Isnull(Gt_46_To_47_Weeks,0) AS Gt_46_To_47_Weeks
, Isnull(Gt_47_To_48_Weeks,0) AS Gt_47_To_48_Weeks
, Isnull(Gt_48_To_49_Weeks,0) AS Gt_48_To_49_Weeks
, Isnull(Gt_49_To_50_Weeks,0) AS Gt_49_To_50_Weeks
, Isnull(Gt_50_To_51_Weeks,0) AS Gt_50_To_51_Weeks
, Isnull(Gt_51_To_52_Weeks,0) AS Gt_51_To_52_Weeks
, Isnull(Gt_52_Weeks,0) AS Gt_52_Weeks
, Isnull(Total,0) AS Total
, Isnull(Patients_with_unknown_clock_start_date,0) AS Patients_with_unknown_clock_start_date
, Total_All
, Effective_Snapshot_Date
 FROM [central_midlands_csu_UserDB].[RTT].[Full_Dataset1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')
	AND Provider_Org_Code = '", Provider_Code, "'", 
      "AND Treatment_Function_Code = '", Treatment_Code, "'",
	"' )",
	con = con
    )
  ) %>% collect()
  
  Provider_List[["RTT_Table_Pivot"]] <- Provider_List[["RTT_Table"]] %>% 
    # mutate_at("RTT_Part_Name", function(x) str_replace_all(x, "PART", "Part")) %>% 
    group_by(RTT_Part_Description, Effective_Snapshot_Date) %>% 
    summarise(sum_Total = sum(Total_All)) %>% 
    pivot_wider(names_from = RTT_Part_Description, values_from = sum_Total, names_repair = "universal")
  Provider_List[["RTT_Table_Pivot"]]$Organisation_Code <- unique(Provider_List[["RTT_Table"]]$Provider_Org_Code)
  Provider_List[["RTT_Table_Pivot"]] %<>% rename("RTT_Referrals" = "New.RTT.Periods...All.Patients")
  
  cat("2/7) RTT pulled\n")
  

  ## Occupied Beds - Daycare Quarterly ####
  
  Provider_List[["Occupied_Beds_Daycare"]] <- tbl(
    con,
    build_sql(
      " SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Specialty
		, Number_Of_Beds AS Number_Of_Beds_DAY
		, Effective_Snapshot_Date
 FROM [central_midlands_csu_UserDB].[Bed_Availability].[Provider_By_Specialty_Occupied_Day_Only_Beds1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'') ",
      "AND Organisation_Code = '", Provider_Code, "' ",
      "AND Specialty = '", as.character(Specialty), "' ",
      "' )",
      con = con
    )
  ) %>% select(-Specialty) %>% collect()
  
  cat("3/7) Occupied Beds Day pulled\n")
  
  ## Occupied Beds - Overnight Quarterly ####
  
  Provider_List[["Occupied_Beds_Overnight"]] <- tbl(
    con,
    build_sql(
      "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Specialty
		, Number_Of_Beds AS Number_Of_Beds_NIGHT
		, Effective_Snapshot_Date
 FROM [central_midlands_csu_UserDB].[Bed_Availability].[Provider_By_Specialty_Occupied_Overnt_Beds1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'') ",
      "AND Organisation_Code = '", Provider_Code, "' ",
      "AND Specialty = '", as.character(Specialty), "' ",
      "' )",
      con = con
    )
  ) %>% select(-Specialty) %>% collect()
  
  cat("4/7) Occupied Beds Overnight pulled\n")
  
  ## Staffing - Medical ####
  
  Provider_List[["Staffing_Medical"]] <- tbl(
    con,
    build_sql(
      "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Health_Education_Region_Code	
		, Organisation_Code	
		, Grade_Sort_Order	
		, Grade	
		, Specialty_Group
		, Specialty	
		, Total_FTE	
		, Effective_Snapshot_Date
FROM [central_midlands_csu_UserDB].[NHS_Workforce].[Medical_Staff1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'') ",
      "AND Organisation_Code = '", Provider_Code, "' ",
      "AND Specialty = '", Specialty_name, "' ",
      "' )"
      , con = con
    )
  ) %>% collect()
  
  Provider_List[["Staffing_Medical_Sum"]] <- Provider_List[["Staffing_Medical"]] %>%
    group_by(Effective_Snapshot_Date, Grade) %>% 
    summarise(sum_FTE = sum(Total_FTE)) %>% 
    pivot_wider(names_from = Grade, values_from = sum_FTE) %>% 
    ungroup()
  Provider_List[["Staffing_Medical_Sum"]]$Organisation_Code <- unique(Provider_List[["Staffing_Medical"]]$Organisation_Code)
  Provider_List[["Staffing_Medical_Sum"]] <- unite(Provider_List[["Staffing_Medical_Sum"]], "Consultant", contains("Consultant")) ## annoyingly unite na.rm only works on chr
  Provider_List[["Staffing_Medical_Sum"]]$Consultant %<>% str_replace_all("_NA|NA_", "")
  Provider_List[["Staffing_Medical_Sum"]]$Consultant %<>% as.numeric()
  Provider_List[["Staffing_Medical_Sum"]] %<>% mutate(Medic_Sum = rowSums(select(., -Effective_Snapshot_Date, -Organisation_Code)))
  Provider_List[["Staffing_Medical_Sum"]] %<>% select(Effective_Snapshot_Date, Organisation_Code, Consultant, Medic_Sum)
  Provider_List[["Staffing_Medical"]] <- NULL
  
  cat("5/7) Medical Staff pulled\n")
  
  ## X Staffing - Non-Medical ####
  
  # Provider_List[["Staffing_NonMed"]] <- tbl(
  #   con,
  #   sql(
  #     "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT    Organisation_Code
  # 		, Main_Staff_Group
  # 		, Staff_Group_1
  # 		, Staff_Group_2
  # 		, Area
  # 		, Level
  # 		, AfC_Band
  # 		, Total_FTE
  # 		, Effective_Snapshot_Date
  # FROM [central_midlands_csu_UserDB].[NHS_Workforce].[NonMedical_Staff1]
  # 	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  #   )
  # ) %>% filter(Organisation_Code == Provider_Code) %>% collect()
  
  ## Staffing HCHS Sickness ####
  
  Provider_List[["Staffing_Sickness"]] <- tbl(
    con,
    build_sql(
      "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Organisation_Type
		, FTE_Days_Sick
		, FTE_Days_Available
		, Effective_Snapshot_Date
FROM [central_midlands_csu_UserDB].[NHS_Workforce].[Sickness_Absence1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'') ",
      "AND Organisation_Code = '",
      Provider_Code,
      "' ",
      "' )",
      con = con
    )
  ) %>% mutate(Absence_PCT = (FTE_Days_Sick / FTE_Days_Available)) %>% select(Effective_Snapshot_Date, Organisation_Code, Absence_PCT) %>% collect()
  
  cat("6/7) Sickness pulled\n")
  
  ## Referrals ####
  
  Provider_List[["Referrals"]] <- tbl(
    con,
    build_sql(
      "SELECT [SPECIALTY_CODE]
      ,[SPECIALTY_DESC]
      ,[SERVICE_PROVIDER_CODE] AS Organisation_Code
      ,[SERVICE_PROVIDER_NAME]
      --,[BOOKING_DATETIME]
      ,[DECISION_TO_REFER_DATE] AS Effective_Snapshot_Date
      ,Count(*) as [ACTIVITY]
      FROM [ERS].[dbo].[tb_ERS_EREFERRALS]",
      "
      WHERE SPECIALTY_CODE = ", as.character(Specialty), "
      AND SERVICE_PROVIDER_CODE = ", Provider_Code,
      "
      GROUP BY [SPECIALTY_CODE]
      ,[SPECIALTY_DESC]
      ,[SERVICE_PROVIDER_CODE]
      ,[SERVICE_PROVIDER_NAME]
      ,[DECISION_TO_REFER_DATE]",
      con = con
    )
  ) %>% collect()

  cat("7/7) Referrals pulled\n")
  
  ## Further wrangling imap for static metrics ####
  
  cat("Doing final wrangling..\n")
  
  Provider_List_Mutate <- Provider_List[which(names(Provider_List) %in% c("Occupied_Beds_Daycare", "Occupied_Beds_Overnight", "Theatres", "Staffing_Medical_Sum", "Staffing_Sickness"))] %>% imap( ~ {
    df <- .x
    df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)
    df <-
      df %>% tidyr::complete(Effective_Snapshot_Date = seq.Date(
        min(.$Effective_Snapshot_Date),
        as.Date("2020-03-31"),
        by = "week"
      ))
    df <-
      df %>% arrange(Effective_Snapshot_Date)
    df$Effective_Snapshot_Date <- df$Effective_Snapshot_Date %>% map_chr(function(x) {
      paste0(year(x), "-", if (str_count(week(x)) == 1) {
        paste0(0, week(x))
      } else {
        week(x)
      }
      )
    }
    )
    
    duplicates <- df %>% group_by(Effective_Snapshot_Date) %>% summarise(hard_count = n()) %>% filter(hard_count != 1) %>% pull(Effective_Snapshot_Date)
    
    df <- df %>% filter(!(Effective_Snapshot_Date %in% duplicates &
                            is.na(Organisation_Code)))
    
    df <- df %>% fill(-"Effective_Snapshot_Date", .direction = "down")
    
    return(df)
  })
  
  ## Further wrangling for referrals ####
  
  df <- Provider_List[["Referrals"]]
  df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)
  # df <- df %>% filter(Effective_Snapshot_Date >= "2019-01-01")
  df <-
    df %>% complete(Effective_Snapshot_Date = seq.Date(
      as.Date("2019-01-01"),
      max(df$Effective_Snapshot_Date),
      by = "day"
    ))
  df <-
    df %>% arrange(Effective_Snapshot_Date)
  
  df$Effective_Snapshot_Date_Year <-
    year(df$Effective_Snapshot_Date)
  df$Effective_Snapshot_Date_Month <-
    month(df$Effective_Snapshot_Date)
  df$Effective_Snapshot_Date_Week <-
    week(df$Effective_Snapshot_Date)
  
  df <-
    df %>% fill(
      "SPECIALTY_CODE",
      "SPECIALTY_DESC",
      "Organisation_Code",
      "SERVICE_PROVIDER_NAME",
      .direction = "down"
    )
  df[which(is.na(df$ACTIVITY)), "ACTIVITY"] <- 0
  
  
  Provider_List_Mutate[["Referrals_MonthWeek"]] <- df %>% group_by(
    Effective_Snapshot_Date_Year,
    Effective_Snapshot_Date_Month,
    Effective_Snapshot_Date_Week
  ) %>%
    summarise(
      Organisation_Code = first(Organisation_Code),
      Activity_Sum = sum(ACTIVITY)
    ) %>%
    arrange(
      Effective_Snapshot_Date_Year,
      Effective_Snapshot_Date_Month,
      Effective_Snapshot_Date_Week
    ) %>%
    mutate(Propn = round(Activity_Sum / sum(Activity_Sum), 3))
  Provider_List_Mutate[["Referrals_MonthWeek"]]$Effective_Snapshot_Date <-
    with(
      Provider_List_Mutate[["Referrals_MonthWeek"]],
      paste0(
        Effective_Snapshot_Date_Year,
        "-",
        Effective_Snapshot_Date_Week %>% map(~ if (str_length(.x) == 1) {
          paste0("0", .x)
        }
        else {
          .x
        })
      )
    )
  
  Provider_List_Mutate[["Referrals_Week"]] <-
    Provider_List_Mutate[["Referrals_MonthWeek"]] %>% group_by(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Week) %>% summarise(
      Organisation_Code = unique(Organisation_Code),
      GP_Referrals = sum(Activity_Sum)
    )
  Provider_List_Mutate[["Referrals_Week"]]$Effective_Snapshot_Date <-
    with(
      Provider_List_Mutate[["Referrals_Week"]],
      paste0(
        Effective_Snapshot_Date_Year,
        "-",
        Effective_Snapshot_Date_Week %>% map(~ if (str_length(.x) == 1) {
          paste0("0", .x)
        } else {
          .x
        })
      )
    )
  Provider_List_Mutate[["Referrals_Week"]] %<>% ungroup() %>% select(Effective_Snapshot_Date, Organisation_Code, GP_Referrals)
  rm(df)
  
  ## Further wrangling - full RTT table ####
  
  Provider_List[["RTT_Table_Pivot"]] <-
    Provider_List[["RTT_Table_Pivot"]] %>%
    mutate_at("Effective_Snapshot_Date", as.Date)
  # Provider_List[["RTT_Table_Pivot"]] <- Provider_List[["RTT_Table_Pivot"]] %>% filter(Effective_Snapshot_Date >= "2019-01-01")
  
  Provider_List[["RTT_Table_Pivot"]]$Effective_Snapshot_Date_Year <-
    year( Provider_List[["RTT_Table_Pivot"]]$Effective_Snapshot_Date)
  Provider_List[["RTT_Table_Pivot"]]$Effective_Snapshot_Date_Month <-
    month(Provider_List[["RTT_Table_Pivot"]]$Effective_Snapshot_Date)
  
  Provider_List_Mutate[["Referrals_MonthWeek"]] <- left_join(
    Provider_List_Mutate[["Referrals_MonthWeek"]],
    Provider_List[["RTT_Table_Pivot"]] %>% select(-Organisation_Code,-Effective_Snapshot_Date),
    by = c(
      "Effective_Snapshot_Date_Year",
      "Effective_Snapshot_Date_Month"
    )
  ) %>% mutate_at(vars("Completed.Pathways.For.Admitted.Patients":"RTT_Referrals"),
                  ~ round(.*Propn, 0))
  
  Provider_List_Mutate[["RTT_PathwayAndReferrals_Complete"]] <-
    Provider_List_Mutate[["Referrals_MonthWeek"]] %>% ungroup() %>% group_by(Effective_Snapshot_Date) %>% summarise(Organisation_Code = unique(Organisation_Code),
                                                                                                                    CompletedPathways_Admitted = sum(Completed.Pathways.For.Admitted.Patients),
                                                                                                                    CompletedPathways_NonAdmitted = sum(Completed.Pathways.For.Non.Admitted.Patients),
                                                                                                                    IncompletePathways = sum(Incomplete.Pathways),
                                                                                                                    IncompletePathways_DTA = sum(Incomplete.Pathways.with.DTA),
                                                                                                                    RTT_Referrals = sum(RTT_Referrals))
  
## Check has cols ####

Check <- Provider_List_Mutate %>% map(~ (
  colnames(.x) %in% c("Effective_Snapshot_Date", "Organisation_Code")
) %>% sum() == 2) %>% as_vector()

## Join ####

## Don't need ReferralsMonthWeek

if (all(Check)) {
Binded <- reduce(Provider_List_Mutate[which(names(Provider_List_Mutate) != "Referrals_MonthWeek")], full_join, c("Effective_Snapshot_Date", "Organisation_Code"))
Binded <- Binded %>% filter(str_detect(Effective_Snapshot_Date, "2019|2020"))
}
  
## Create Derived columns ####
  
Binded %<>% mutate(OtherReferrals = RTT_Referrals-GP_Referrals,
                  NoAdm_per_Bed = round(CompletedPathways_Admitted/(Number_Of_Beds_DAY+Number_Of_Beds_NIGHT), 2),
                  NoAdm_per_Consultant = round(CompletedPathways_Admitted/Consultant/(1-Absence_PCT), 2),
                  NoAdm_per_Theatre = round(CompletedPathways_Admitted/No_of_Operating_Theatres, 2),
                  NoSeen_per_Consultant = round(CompletedPathways_NonAdmitted/Consultant/(1-Absence_PCT), 2),)
  
## Time end ####
  
  end <- Sys.time()
  
  cat(end-start)

return(Binded)
}

## Parameters ####

Provider_Code <- "RL4"
Specialty <- 110
Specialty_name <- "Trauma and orthopaedic surgery"

## Use function ####

outpatients <- Pull_From_SQL(Provider_Code = Provider_Code,
                      Specialty = Specialty,
                      Treatment_Code = paste0("C_", Specialty),
                      Specialty_name = Specialty_name)

## Write to CSV ####

write_csv(outpatients, paste0(Provider_Code, "_", str_replace_all(Specialty_name, " ", "_"), ".csv"))

## Available Specialties ####

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

## Ignore This ####

# df <- Provider_List[["Staffing_Sickness"]]
# df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)
# df <-
#   df %>% tidyr::complete(Effective_Snapshot_Date = seq.Date(
#     min(df$Effective_Snapshot_Date),
#     max(df$Effective_Snapshot_Date),
#     by = "week"
#   ))
# df <-
#   df %>% arrange(Effective_Snapshot_Date) %>% fill(-"Effective_Snapshot_Date", .direction = "down")
# df %<>% mutate(AbsenseRate = )

## Save ####

save.image("workspace.RData")
