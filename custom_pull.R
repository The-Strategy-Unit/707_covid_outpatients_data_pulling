library(tidyverse)
library(dbplyr)
library(DBI)
library(odbc)
library(janitor)
library(magrittr)
library(lubridate)

## Establish Connection ####

con <-
  dbConnect(odbc(), Driver = "SQL Server", server = "MLCSU-BI-SQL-SU")

## Create Empty List Container ####

Provider_List <- list()

## Theatres Quarterly Data ####

Provider_List[["Theatres"]] <- tbl(
  con,
  sql(
    "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Effective_Snapshot_Date
		, No_of_Operating_Theatres
		, No_of_Operating_Theatres_Day_Case_Only
	FROM [central_midlands_csu_UserDB].[Cancelled_Elec_Ops].[Supporting_Facilities1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  )
) %>% filter(Organisation_Code == "RL4") %>% collect()

## Cancelled Elective Operations ####

Provider_List[["Cancelled_Elective_Ops"]] <- tbl(
  con, 
  sql(
    "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT 
     Provider_Code AS Organisation_Code
		,Last_Minute_Cancelled_Ops_Non_Clinical_Reason
		,Number_Of_Cancellations_On_The_Day_Of_Surgery
		,Patients_Not_Treated_Within_X_Of_Cancellation
		,X
		,Effective_Snapshot_Date
 FROM [central_midlands_csu_UserDB].[Cancelled_Elec_Ops].[Cancelled_Elective_Ops_By_Provider1]#
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  )
  ) %>% filter(Organisation_Code == "RL4") %>% collect()
                                            
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
# ) %>% filter(Organisation_Code == "RL4", Treatment_Function_Code == 100) %>% collect()
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
# ) %>% filter(Organisation_Code == "RL4") %>% collect()

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
# ) %>% filter(Organisation_Code == "RL4") %>% collect()

## RTT Referrals ####

Provider_List[["RTT_Referrals"]] <- tbl(
  con,
  sql(
    "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Treatment_Function_Code
		, Number_Of_Incomplete_Pathways
		, Number_Of_Incomplete_Pathways_With_DTA_For_Treatment
		, Number_Of_New_RTT_Clock_Starts_In_Month	Report_Period_Length
		, Effective_Snapshot_Date
	FROM [central_midlands_csu_UserDB].[RTT].[New_Data_Items_Provider1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  )
) %>% filter(Organisation_Code == "RL4", Treatment_Function_Code == "110") %>% collect()
Provider_List[["RTT_Referrals"]] <- Provider_List[["RTT_Referrals"]] %>% select(Effective_Snapshot_Date, Organisation_Code, Report_Period_Length) %>% rename(Num_RTT_Referrals = Report_Period_Length)

## RTT Full Table ####

Provider_List[["RTT_Table"]] <- tbl(
  con,
  sql(
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
	and Provider_Org_Code = ''RL4''
	' )"
  )
) %>% collect()

Provider_List[["RTT_Table_Pivot"]] <- Provider_List[["RTT_Table"]] %>% group_by(RTT_Part_Name, Effective_Snapshot_Date) %>% summarise(sum_Total = sum(Total_All)) %>% pivot_wider(names_from = RTT_Part_Name, values_from = sum_Total)
Provider_List[["RTT_Table_Pivot"]]$Organisation_Code <- unique(Provider_List[["RTT_Table"]]$Provider_Org_Code)
Provider_List[["RTT_Table"]] <- NULL

## Occupied Beds - Daycare Quarterly ####

Provider_List[["Occupied_Beds_Daycare"]] <- tbl(
  con,
  sql(
  " SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Specialty
		, Number_Of_Beds AS Number_Of_Beds_DAY
		, Effective_Snapshot_Date
 FROM [central_midlands_csu_UserDB].[Bed_Availability].[Provider_By_Specialty_Occupied_Day_Only_Beds1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
      )
) %>% filter(Organisation_Code == "RL4", Specialty == "110") %>% collect()

## Occupied Beds - Overnight Quarterly ####

Provider_List[["Occupied_Beds_Overnight"]] <- tbl(
  con,
sql(
  "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Specialty
		, Number_Of_Beds AS Number_of_Beds_NIGHT
		, Effective_Snapshot_Date
 FROM [central_midlands_csu_UserDB].[Bed_Availability].[Provider_By_Specialty_Occupied_Overnt_Beds1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
)
) %>% filter(Organisation_Code == "RL4", Specialty == "110") %>% collect()

## Staffing - Medical ####

Provider_List[["Staffing_Medical"]] <- tbl(
  con,
  sql(
    "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Health_Education_Region_Code	
		, Organisation_Code	
		, Grade_Sort_Order	
		, Grade	
		, Specialty_Group
		, Specialty	
		, Total_FTE	
		, Effective_Snapshot_Date
FROM [central_midlands_csu_UserDB].[NHS_Workforce].[Medical_Staff1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  )
) %>% filter(Organisation_Code == "RL4") %>% collect()

Provider_List[["Staffing_Medical_Sum"]] <- Provider_List[["Staffing_Medical"]] %>% group_by(Effective_Snapshot_Date, Grade) %>% summarise(sum_FTE = sum(Total_FTE)) %>% pivot_wider(names_from = Grade, values_from = sum_FTE) %>% ungroup()
Provider_List[["Staffing_Medical_Sum"]]$Organisation_Code <- unique(Provider_List[["Staffing_Medical"]]$Organisation_Code)
Provider_List[["Staffing_Medical"]] <- NULL

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
# ) %>% filter(Organisation_Code == "RL4") %>% collect()

## Staffing HCHS Sickness ####

Provider_List[["Staffing_Sickness"]] <- tbl(
  con,
  sql(
    "SELECT * FROM OPENQUERY ( [FD_UserDB] ,'SELECT Organisation_Code
		, Organisation_Type
		, FTE_Days_Sick
		, FTE_Days_Available
		, Effective_Snapshot_Date
FROM [central_midlands_csu_UserDB].[NHS_Workforce].[Sickness_Absence1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')' )"
  )
) %>% filter(Organisation_Code == "RL4") %>% collect()

## Referrals ####

Provider_List[["Referrals"]] <- tbl(
  con,
  sql(
    "SELECT [SPECIALTY_CODE]
      ,[SPECIALTY_DESC]
      ,[SERVICE_PROVIDER_CODE] AS Organisation_Code
      ,[SERVICE_PROVIDER_NAME]
      --,[BOOKING_DATETIME]
      ,[DECISION_TO_REFER_DATE] AS Effective_Snapshot_Date
      ,Count(*) as [ACTIVITY]
      FROM [ERS].[dbo].[tb_ERS_EREFERRALS]
      GROUP BY [SPECIALTY_CODE]
      ,[SPECIALTY_DESC]
      ,[SERVICE_PROVIDER_CODE]
      ,[SERVICE_PROVIDER_NAME]
      ,[DECISION_TO_REFER_DATE]
    "
  )
) %>% filter(SPECIALTY_CODE == "110", Organisation_Code == "RL4") %>% collect()

## Further wrangling ####

Provider_List_Mutate <- Provider_List %>% imap( ~ {
  df <- .x
  df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)
  df <-
    df %>% tidyr::complete(Effective_Snapshot_Date = seq.Date(
      min(df$Effective_Snapshot_Date),
      max(df$Effective_Snapshot_Date),
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
  
  
if (.y == "Referrals") {
  df <- df %>% group_by(Effective_Snapshot_Date) %>% summarise(Referrals_Sum = sum(ACTIVITY))
  df[which(is.na(df$Referrals_Sum)),"Referrals_Sum"] <- 0
} else {
  df <-
    df %>% fill(-"Effective_Snapshot_Date", .direction = "down")
}
  return(df)
})

## Join

left_join(
  Provider_List_Mutate[["Occupied_Beds_Daycare"]] %>% select(-"Specialty"),
  Provider_List_Mutate[["Occupied_Beds_Overnight"]],
  by = c("Effective_Snapshot_Date", "Organisation_Code")
) %>% left_join(
  Provider_List_Mutate[["Staffing_Sickness"]] %>% select(-"Organisation_Type")
) %>% left_join(
  Provider_List_Mutate[["Cancelled_Elective_Ops"]] %>% select(-"Number_Of_Cancellations_On_The_Day_Of_Surgery", -"X")
) %>% left_join(
  Provider_List_Mutate[["Theatres"]]
)

## 

df <- Provider_List[["Staffing_Sickness"]]
df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)
df <-
  df %>% tidyr::complete(Effective_Snapshot_Date = seq.Date(
    min(df$Effective_Snapshot_Date),
    max(df$Effective_Snapshot_Date),
    by = "week"
  ))
df <-
  df %>% arrange(Effective_Snapshot_Date) %>% fill(-"Effective_Snapshot_Date", .direction = "down")

## Save ####

save.image("workspace.RData")
