############################
## Relevant Specialties ####
############################

outpatients_specialties <- c(100, 101, 110, 120, 130, 140, 150, 160, 170, 300, 301, 320, 330, 340, 400, 410, 430, 502, "X01")
outpatients_regex <- outpatients_specialties[-length(outpatients_specialties)] %>% paste0(collapse = "'', ''")

outpatient_specialty_names <- c("General surgery", "Urology", "Trauma and orthopaedic surgery", "Otolaryngology", "Ophthalmology", "Oral and maxillo-facial surgery", "Oral and Maxillofacial Surgery", "Oral Surgery", "Neurosurgery", "Plastic surgery", "Cardio-thoracic surgery", "General (internal) medicine", "Gastro-enterology", "Gastroenterology", "Cardiology", "Dermatology", "Respiratory medicine", "Neurology", "Rheumatology", "Geriatric medicine", "Obstetrics and Gynaecology")
outpatient_specialty_names_regex <- outpatient_specialty_names %>% paste0(collapse = "'', ''")

outpatients_tibble <- tibble(codes = outpatients_specialties,
                             code_names = c("General surgery", "Urology", "Trauma and orthopaedic surgery", "Otolaryngology", "Ophthalmology", "Oral surgery", "Neurosurgery", "Plastic surgery", "Cardio-thoracic surgery", "General (internal) medicine", "Gastroenterology", "Cardiology", "Dermatology", "Respiratory medicine", "Neurology", "Rheumatology", "Geriatric medicine", "Obstetrics and Gynaecology", "Other"))


##################################
## Look up for relevant dates ####
##################################

dates_lookup <- tibble(day = seq.Date(as.Date("2018-01-01"),
                                      as.Date("2050-04-05"),
                                      by = "day"),
                       formattedweek = day %>% map_chr(function(x) {
                         paste0(year(x), "-", if (str_count(week(x)) == 1) {
                           paste0(0, week(x))
                         } else {
                           week(x)
                         }
                         )
                       }),
                       week = week(day),
                       month = month(day),
                       year = year(day)
)

dates_lookup_formula <- dates_lookup %>% group_by(year, month, week) %>% summarise(nrows = n())
dates_lookup_formula %<>% mutate(days_in_month = sum(nrows), 
                                 days_in_month_propn = nrows/days_in_month,
                                 Effective_Snapshot_Date = paste0(
                                   year,
                                   "-",
                                   week %>% map(~ if (str_length(.x) == 1) {
                                     paste0("0", .x)
                                   }
                                   else {
                                     .x
                                   })
                                 )
)

#####################
## Load Diag CSV ####
#####################

DiagSpecSplits <- read_csv(url("https://raw.githubusercontent.com/The-Strategy-Unit/707_covid_outpatients_data_pulling/master/DiagSpecSplits.csv"))

DiagSpecSplits <- bind_rows(DiagSpecSplits,
                            crossing(Test = DiagSpecSplits$Test, Specialty = setdiff(outpatients_tibble$codes, DiagSpecSplits$Specialty), Percent = 0.00))

########################
## Load Trust Codes ####
########################

TrustCodes <- read_csv(url("https://raw.githubusercontent.com/The-Strategy-Unit/707_covid_outpatients_data_pulling/master/TrustCodeswithRegions.csv"))
TrustCodes <- TrustCodes %>% rename(Region_Code = RegionCode)

Pull_From_SQL_Regional <- function(RegionCode, Specialty, Treatment_Code, Specialty_name, Final_Date) {
  
TrustCodesRegex <- TrustCodes %>% filter(Region_Code == RegionCode) %>% pull(TrustCode) %>% paste0(collapse = "', '")
TrustCodesRegex_00 <- TrustCodes %>% filter(Region_Code == RegionCode) %>% pull(TrustCode) %>% paste0("00", collapse = "', '")

## Run extraction process ####

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
	FROM [central_midlands_csu_UserDB].[Cancelled_Elec_Ops].[Supporting_Facilities1]
	WHERE Left(Effective_Snapshot_Date,4) in (''2018'',''2019'',''2020'')
	AND Organisation_Code IN ('", TrustCodesRegex, "')", "
      '
      )",
    con = con
  )
) %>% collect()

Provider_List[["Theatres"]] %<>% group_by(Effective_Snapshot_Date) %>% summarise(No_of_Operating_Theatres = sum(No_of_Operating_Theatres))

cat("1/9) Theatres pulled\n")

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
	AND Provider_Org_Code IN ('", TrustCodesRegex, "')", "
  AND Treatment_Function_Code = '", Treatment_Code, "'",
    "
      ' )",
    con = con
  )
) %>% collect()

Provider_List[["RTT_Table_Pivot"]] <- Provider_List[["RTT_Table"]] %>% 
  # mutate_at("RTT_Part_Name", function(x) str_replace_all(x, "PART", "Part")) %>% 
  group_by(RTT_Part_Description, Effective_Snapshot_Date) %>% 
  summarise(sum_Total = sum(Total_All)) %>% 
  pivot_wider(names_from = RTT_Part_Description, values_from = sum_Total, names_repair = "universal")
# Provider_List[["RTT_Table_Pivot"]]$Organisation_Code <- unique(Provider_List[["RTT_Table"]]$Provider_Org_Code)
Provider_List[["RTT_Table_Pivot"]] %<>% rename("RTT_Referrals" = "New.RTT.Periods...All.Patients")
Provider_List[["RTT_Table_Pivot"]] %<>% arrange(Effective_Snapshot_Date)
Provider_List[["RTT_Table_Pivot"]] %<>% map_at(c("Completed.Pathways.For.Admitted.Patients",
                                                 "Completed.Pathways.For.Non.Admitted.Patients",
                                                 "Incomplete.Pathways",
                                                 "Incomplete.Pathways.with.DTA",
                                                 "RTT_Referrals"), ~ {
                                                   vector <- .x
                                                   for (idx in min(which(!is.na(vector))):max(which(!is.na(vector)))) {
                                                     if (is.na(vector[idx]))
                                                       vector[idx] <- 0L
                                                   }
                                                   return(vector)
                                                 }) %>% as_tibble()

for (var in c("Completed.Pathways.For.Admitted.Patients",
              "Completed.Pathways.For.Non.Admitted.Patients",
              "Incomplete.Pathways",
              "Incomplete.Pathways.with.DTA",
              "RTT_Referrals")) {
  
  if (var %in% colnames(Provider_List[["RTT_Table_Pivot"]]) == FALSE) {
    
    Provider_List[["RTT_Table_Pivot"]][[var]] <- 0
  }
}

Provider_List[["RTT_Table_Pivot"]] %<>% select(
  "Effective_Snapshot_Date",
  "Completed.Pathways.For.Admitted.Patients",
  "Completed.Pathways.For.Non.Admitted.Patients",
  "Incomplete.Pathways",
  "Incomplete.Pathways.with.DTA",
  "RTT_Referrals"
  # ,
  # "Organisation_Code"
)



cat("2/9) RTT pulled\n")


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
    "AND Organisation_Code IN ('", TrustCodesRegex, "')",
    if (Specialty == "X01") {
      sql(paste0("AND Specialty NOT IN (''", outpatients_regex, "'') "))
    } else if (Specialty == "170" | Specialty == 170) {
      sql(paste0("AND Specialty IN (''170'', ''172'', ''173'') "))
    } else if (Specialty == "110" | Specialty == 110) {
      sql(paste0("AND Specialty IN (''108'', ''110'') "))
    } else {
      sql(paste0("AND Specialty = ''", as.character(Specialty), "'' "))
    },
    "' )",
    con = con
  )
) %>% select(-Specialty) %>% collect()

Provider_List[["Occupied_Beds_Daycare"]] %<>% group_by(Effective_Snapshot_Date) %>% summarise(Number_Of_Beds_DAY = sum(Number_Of_Beds_DAY))

cat("3/9) Occupied Beds Day pulled\n")

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
    "AND Organisation_Code IN ('", TrustCodesRegex, "')",
    if (Specialty == "X01") {
      sql(paste0("AND Specialty NOT IN (''", outpatients_regex, "'') "))
    } else if (Specialty == "170" | Specialty == 170) {
      sql(paste0("AND Specialty IN (''170'', ''172'', ''173'') "))
    } else if (Specialty == "110" | Specialty == 110) {
      sql(paste0("AND Specialty IN (''108'', ''110'') "))
    } else {
      sql(paste0("AND Specialty = ''", as.character(Specialty), "'' "))
    },
    "' )",
    con = con
  )
) %>% select(-Specialty) %>% collect()

Provider_List[["Occupied_Beds_Overnight"]] %<>% group_by(Effective_Snapshot_Date) %>% summarise(Number_Of_Beds_NIGHT = sum(Number_Of_Beds_NIGHT))

cat("4/9) Occupied Beds Overnight pulled\n")

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
    "AND Organisation_Code IN ('",
    TrustCodesRegex,
    "')",
    if (Specialty_name == "Other") {
      sql(paste0(
        "AND Specialty NOT IN ('",
        "'",
        outpatient_specialty_names_regex,
        "'" 
      ))
    } else {
      if (Specialty_name %in% c("Gastroenterology", "Gastro-enterology")) {
        sql(paste0("AND Specialty in (''Gastroenterology'', ''gastro-enterology'"))
      } else if (Specialty_name %in% c("Oral and Maxillofacial Surgery",
                                       "Oral and maxillo-facial surgery",
                                       "Oral Surgery")) {
        sql(paste0("AND Specialty in (''Oral and Maxillofacial Surgery'', ''Oral and maxillo-facial surgery'', ''Oral Surgery'"))
      } else {
        sql(paste0("AND Specialty in (''", Specialty_name, "'"))
      }
    }
    ,
    "') ",
    "' )"
    ,
    con = con
  )
) %>% collect()

Provider_List[["Staffing_Medical_Sum"]] <- Provider_List[["Staffing_Medical"]] %>%
  group_by(Effective_Snapshot_Date, Grade) %>% 
  summarise(sum_FTE = sum(Total_FTE)) %>% 
  pivot_wider(names_from = Grade, values_from = sum_FTE) %>% 
  ungroup()

walk(c("Associate Specialist", "Consultant (including Directors of Public Health)", 
       "Core Training", "Foundation Doctor Year 1", "Foundation Doctor Year 2", 
       "Specialty Doctor", "Specialty Registrar", "Consultant"), ~ {
         
         if (!.x %in% names(Provider_List[["Staffing_Medical_Sum"]])) {
           Provider_List[["Staffing_Medical_Sum"]] <<- mutate(Provider_List[["Staffing_Medical_Sum"]], !!.x := 0)
         }
       })

# Provider_List[["Staffing_Medical_Sum"]]$Organisation_Code <- unique(Provider_List[["Staffing_Medical"]]$Organisation_Code)
Provider_List[["Staffing_Medical_Sum"]] <- unite(Provider_List[["Staffing_Medical_Sum"]], "Consultant", contains("Consultant")) ## annoyingly unite na.rm only works on chr
Provider_List[["Staffing_Medical_Sum"]]$Consultant %<>% str_replace_all("_NA|NA_", "")
suppressWarnings({
  Provider_List[["Staffing_Medical_Sum"]]$Consultant %<>% as.numeric()
})

if (all(is.na(Provider_List[["Staffing_Medical_Sum"]]$Consultant))) {
  Provider_List[["Staffing_Medical_Sum"]]$Consultant <- 0
}

Provider_List[["Staffing_Medical_Sum"]] %<>% mutate(Medic_Sum = rowSums(
  select(.,-Effective_Snapshot_Date
         # ,-Organisation_Code
  ),
  na.rm = T
))
Provider_List[["Staffing_Medical_Sum"]] %<>% select(Effective_Snapshot_Date, 
                                                    # Organisation_Code, 
                                                    Consultant, Medic_Sum)
Provider_List[["Staffing_Medical"]] <- NULL

cat("5/9) Medical Staff pulled\n")

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
    "AND Organisation_Code IN ('",
    TrustCodesRegex,
    "')",
    "' )",
    con = con
  )
) %>% group_by(Effective_Snapshot_Date) %>% summarise(FTE_Days_Sick = sum(FTE_Days_Sick, na.rm = T), FTE_Days_Available = sum(FTE_Days_Available, na.rm = T)) %>% mutate(Absence_PCT = (FTE_Days_Sick / FTE_Days_Available)) %>% select(Absence_PCT, Effective_Snapshot_Date) %>% collect()

cat("6/9) Sickness pulled\n")

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
      WHERE SERVICE_PROVIDER_CODE IN ('",
    sql(TrustCodesRegex), "')",
    if (Specialty == "X01") {
      sql(paste0(
        "
                 AND SPECIALTY_CODE NOT IN ('",
        str_replace_all(outpatients_regex, "''", "'"),
        "') "
      ))
    } else if (Specialty == "170" | Specialty == 170) {
      sql(paste0(
        "
                 AND SPECIALTY_CODE IN ('",
        str_replace_all("170'', ''172'', ''173", "''", "'"),
        "') "
      ))
    } else {
      sql(paste0("AND SPECIALTY_CODE = ", "'", as.character(Specialty), "'"))
    },
    "
      GROUP BY [SPECIALTY_CODE]
      ,[SPECIALTY_DESC]
      ,[SERVICE_PROVIDER_CODE]
      ,[SERVICE_PROVIDER_NAME]
      ,[DECISION_TO_REFER_DATE]",
    con = con
  )
) %>% collect()

Provider_List[["Referrals"]] %<>% group_by(Effective_Snapshot_Date) %>% summarise(ACTIVITY = sum(ACTIVITY))

Provider_List[["Referrals"]] %<>% filter(!is.na(Effective_Snapshot_Date))

cat("7/9) Referrals pulled\n")

## Patient Left Not Seen (NotSeen_Recovered) ####

Provider_List[["NotSeen_Recovered"]] <- tbl(
  con,
  build_sql(
    "SELECT *
        FROM (
          SELECT 
          ProviderCode
          , ProviderName
          , TreatmentSpecialtyCode
          , TreatmentSpecialtyDescription
          , [AppointmentDate]
          , Count(*) AS Activity
          FROM [EAT_Reporting].[dbo].[tbOutpatient]
          WHERE [PathwayReferralToTreatmentStatusCode] ='35'
          AND Iscosted = 1
          AND IsExcluded = 0
          AND [AppointmentDate] >= '2019-01-01 00:00:00.000'
          GROUP BY  ProviderCode
          , ProviderName
          , TreatmentSpecialtyCode
          , TreatmentSpecialtyDescription
          , [AppointmentDate]
          
          UNION ALL
          
          SELECT 
          ProviderCode
          , ProviderName
          , TreatmentSpecialtyCode
          , TreatmentSpecialtyDescription
          , DischargeDate
          , Count(*) AS Activity
          FROM [EAT_Reporting].[dbo].[tbInpatientEpisodes]
          WHERE [PathwayReferralToTreatmentStatusCode] ='35'
          AND IsDominant = 1
          AND Isexcluded = 0
          AND Iscosted = 1
          AND DischargeDate >= '2019-01-01 00:00:00.000'
          GROUP BY  ProviderCode
          , ProviderName
          , TreatmentSpecialtyCode
          , TreatmentSpecialtyDescription
          , DischargeDate
        ) AS DF
         WHERE DF.ProviderCode IN ('", sql(TrustCodesRegex_00), "')",
    if (Specialty == "X01") {
      sql(paste0("
                 AND DF.TreatmentSpecialtyCode NOT IN ('", str_replace_all(outpatients_regex, "''", "'"), "') "))
    } else {
      sql(paste0("AND DF.TreatmentSpecialtyCode = ", "'", as.character(Specialty), "'"))
    },
    con = con)) %>% rename("Effective_Snapshot_Date" = "AppointmentDate") %>% collect()

Provider_List[["NotSeen_Recovered"]] %<>% group_by(Effective_Snapshot_Date) %>% summarise(Activity = sum(Activity))

cat("8/9) NotSeen_Recovered pulled\n")

## Patient Died Before Treatment ####

Provider_List[["NotSeen_Died"]] <- tbl(
  con,
  build_sql(
    " SELECT *
 FROM (
 SELECT 
		  ProviderCode
		, ProviderName
		, TreatmentSpecialtyCode
		, TreatmentSpecialtyDescription
		, [AppointmentDate]
		, Count(*) AS Activity
  FROM [EAT_Reporting].[dbo].[tbOutpatient]
  WHERE [PathwayReferralToTreatmentStatusCode] ='36'
		AND Iscosted = 1
		AND IsExcluded = 0
		AND [AppointmentDate] >= '2019-01-01 00:00:00.000'
 GROUP BY  ProviderCode
		, ProviderName
		, TreatmentSpecialtyCode
		, TreatmentSpecialtyDescription
		, [AppointmentDate]

UNION ALL

SELECT 
		  ProviderCode
		, ProviderName
		, TreatmentSpecialtyCode
		, TreatmentSpecialtyDescription
		, DischargeDate
		, Count(*) AS Activity
  FROM [EAT_Reporting].[dbo].[tbInpatientEpisodes]
  WHERE [PathwayReferralToTreatmentStatusCode] ='36'
		AND IsDominant = 1
		AND Isexcluded = 0
		AND Iscosted = 1
		AND DischargeDate >= '2019-01-01 00:00:00.000'
 GROUP BY  ProviderCode
		, ProviderName
		, TreatmentSpecialtyCode
		, TreatmentSpecialtyDescription
		, DischargeDate
		) AS DF
    WHERE DF.ProviderCode IN ('", sql(TrustCodesRegex_00), "')",
    if (Specialty == "X01") {
      sql(paste0("
                 AND DF.TreatmentSpecialtyCode NOT IN ('", str_replace_all(outpatients_regex, "''", "'"), "') "))
    } else {
      sql(paste0("
                 AND DF.TreatmentSpecialtyCode = ", "'", as.character(Specialty), "'"))
    },
    con = con)) %>% rename("Effective_Snapshot_Date" = "AppointmentDate") %>% collect()

cat("9/9) NotSeen_Died pulled\n")

Provider_List[["NotSeen_Died"]] %<>% group_by(Effective_Snapshot_Date) %>% summarise(Activity = sum(Activity))

## Further wrangling imap for static metrics ####

cat("Doing final wrangling..\n")

Provider_List_Mutate <- Provider_List[which(names(Provider_List) %in% c("Occupied_Beds_Daycare", "Occupied_Beds_Overnight", "Theatres", "Staffing_Medical_Sum", "Staffing_Sickness"))] %>% imap( ~ {
  df <- .x
  df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)
  
  if (.y == c("Occupied_Beds_Daycare") & Specialty == "X01") {
    df <- df %>% group_by(Effective_Snapshot_Date) %>% summarise(Number_Of_Beds_DAY = sum(Number_Of_Beds_DAY))
  }
  if (.y == c("Occupied_Beds_Overnight") & Specialty == "X01") {
    df <- df %>% group_by(Effective_Snapshot_Date) %>% summarise(Number_Of_Beds_NIGHT = sum(Number_Of_Beds_NIGHT))
  }    
  
  
  df <-
    df %>% tidyr::complete(Effective_Snapshot_Date = seq.Date(
      min(.$Effective_Snapshot_Date),
      as.Date(Final_Date, format = "%Y-%m-%d"),
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
  
  temp_col_name <- colnames(df)[2]
  
  df <- df %>% filter(!(Effective_Snapshot_Date %in% duplicates & is.na(!!sym(temp_col_name))))
  
  df <- df %>% fill(-"Effective_Snapshot_Date", .direction = if (.y %in% c("Occupied_Beds_Daycare", "Occupied_Beds_Overnight", "Staffing_Medical_Sum")) "up" else "down")
  
  return(df)
})

## Further Wrangling for Patient NotSeen_Dead + NotSeen_Recovered ####

walk(c("NotSeen_Recovered", "NotSeen_Died"), ~ {
  df <- Provider_List[[.x]]
  df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)
  # df <- df %>% filter(Effective_Snapshot_Date >= "2019-01-01")
  df <-
    df %>% complete(Effective_Snapshot_Date = seq.Date(
      as.Date("2019-01-01"),
      as.Date(Final_Date),
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

    df[which(is.na(df$Activity)), "Activity"] <- 0
  
  df <- df %>% group_by(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Week) %>% summarise(
    !!paste0(.x) := sum(Activity)
  )
  df$Effective_Snapshot_Date <-
    with(
      df,
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
  df %<>% ungroup() %>% select(Effective_Snapshot_Date, .x)
  
  Provider_List_Mutate[[paste0(.x)]] <<- df
}
)

## Further wrangling for referrals ####

df <- Provider_List[["Referrals"]]
df <- df %>% mutate_at("Effective_Snapshot_Date", as.Date)

if (Specialty == "X01") {
  df <- df %>% group_by(Effective_Snapshot_Date) %>% summarise(ACTIVITY = sum(ACTIVITY))
}

# df <- df %>% filter(Effective_Snapshot_Date >= "2019-01-01")
df <-
  df %>% complete(Effective_Snapshot_Date = seq.Date(
    from = as.Date("2018-01-01"),
    to = if (nrow(df) == 0) as.Date(Final_Date) else max(df$Effective_Snapshot_Date),
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

for (var in c("SPECIALTY_CODE",
              "SPECIALTY_DESC",
              "Organisation_Code",
              "SERVICE_PROVIDER_NAME")) {
  if (var %in% colnames(df)) {
    df <-
      df %>% fill(
        var,
        .direction = "down"
      )
  }
  
}

df[which(is.na(df$ACTIVITY)), "ACTIVITY"] <- 0

Provider_List_Mutate[["Referrals_MonthWeek"]] <- df %>% group_by(
  Effective_Snapshot_Date_Year,
  Effective_Snapshot_Date_Month,
  Effective_Snapshot_Date_Week
) %>%
  summarise(
    Activity_Sum = sum(ACTIVITY)
  ) %>%
  arrange(
    Effective_Snapshot_Date_Year,
    Effective_Snapshot_Date_Month,
    Effective_Snapshot_Date_Week
  ) %>%
  mutate(Propn = round(Activity_Sum / sum(Activity_Sum), 3))

if (all(is.nan(Provider_List_Mutate[["Referrals_MonthWeek"]] %>% pull(Propn)))) {
  
  for (index in seq_len(nrow(Provider_List_Mutate[["Referrals_MonthWeek"]]))) {
    
    my_year <- Provider_List_Mutate[["Referrals_MonthWeek"]][index,] %>% pull(Effective_Snapshot_Date_Year)
    my_month <- Provider_List_Mutate[["Referrals_MonthWeek"]][index,] %>% pull(Effective_Snapshot_Date_Month)
    my_week <- Provider_List_Mutate[["Referrals_MonthWeek"]][index,] %>% pull(Effective_Snapshot_Date_Week)
    
    Provider_List_Mutate[["Referrals_MonthWeek"]][[index,"Propn"]] <- dates_lookup_formula %>% filter(year == my_year, month == my_month, week == my_week) %>% pull(days_in_month_propn)
    
  }
  
  rm(my_year, my_month, my_week)
  
}

## ####

## Fix April Proportions

april_propn_fix <- anti_join(dates_lookup_formula %>% filter(year == 2020, month == 4) %>% select(year, month, week),
          Provider_List_Mutate[["Referrals_MonthWeek"]] %>%
            ungroup %>% 
            select(contains("Effective")) %>% 
            filter(Effective_Snapshot_Date_Year == 2020, 
                   Effective_Snapshot_Date_Month == 4) %>% 
            rename(year = Effective_Snapshot_Date_Year,
                   month = Effective_Snapshot_Date_Month,
                   week = Effective_Snapshot_Date_Week)
) %>% ungroup()

if (nrow(april_propn_fix) > 0) {
  
  for (idx in seq_len(nrow(april_propn_fix))) {
    Provider_List_Mutate[["Referrals_MonthWeek"]] <- 
      add_row(Provider_List_Mutate[["Referrals_MonthWeek"]] %>% ungroup(),
              Effective_Snapshot_Date_Year = april_propn_fix[[idx, "year"]],
              Effective_Snapshot_Date_Month = april_propn_fix[[idx, "month"]],
              Effective_Snapshot_Date_Week = april_propn_fix[[idx, "week"]],
              Activity_Sum = 0)
  }
}

if (any(is.nan(Provider_List_Mutate[["Referrals_MonthWeek"]] %>% pull(Propn)))) {
  
  for (row_idx in which(is.nan(Provider_List_Mutate[["Referrals_MonthWeek"]] %>% pull(Propn)))) {
    
    my_year <- Provider_List_Mutate[["Referrals_MonthWeek"]][row_idx,] %>% pull(Effective_Snapshot_Date_Year)
    my_month <- Provider_List_Mutate[["Referrals_MonthWeek"]][row_idx,] %>% pull(Effective_Snapshot_Date_Month)
    my_week <- Provider_List_Mutate[["Referrals_MonthWeek"]][row_idx,] %>% pull(Effective_Snapshot_Date_Week)
    
    Provider_List_Mutate[["Referrals_MonthWeek"]][[row_idx,"Propn"]] <- dates_lookup_formula %>% filter(year == my_year, month == my_month, week == my_week) %>% pull(days_in_month_propn)
    
  }
  
  rm(my_year, my_month, my_week)
}

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

## April propns fix assign ####

for (april_week in paste0("2020-", 14:18)) {
  Provider_List_Mutate[["Referrals_MonthWeek"]][which(Provider_List_Mutate[["Referrals_MonthWeek"]]$Effective_Snapshot_Date == april_week),"Propn"] <- dates_lookup_formula %>% filter(Effective_Snapshot_Date == april_week & month == 4) %>% pull(days_in_month_propn)
}

## Above ##

Provider_List_Mutate[["Referrals_Week"]] <-
  Provider_List_Mutate[["Referrals_MonthWeek"]] %>% group_by(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Week) %>% summarise(
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

Provider_List_Mutate[["Referrals_Week"]] %<>% ungroup() %>% select(Effective_Snapshot_Date, GP_Referrals)
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

missing_months <- anti_join(Provider_List[["RTT_Table_Pivot"]] %>% select(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Month), Provider_List_Mutate[["Referrals_MonthWeek"]] %>% select(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Month))

if (nrow(missing_months) > 0) {
  
  Provider_List_Mutate[["Referrals_MonthWeek"]] <- bind_rows(Provider_List_Mutate[["Referrals_MonthWeek"]],
                                                             map2_df(missing_months$Effective_Snapshot_Date_Year, missing_months$Effective_Snapshot_Date_Month,
                                                                     ~ {
                                                                       
                                                                       dates_lookup_formula %>% filter(year == .x, month == .y) %>%
                                                                         select(-nrows, -days_in_month) %>% 
                                                                         rename(
                                                                           Effective_Snapshot_Date_Year = year,
                                                                           Effective_Snapshot_Date_Month = month,
                                                                           Effective_Snapshot_Date_Week = week,
                                                                           Propn = days_in_month_propn)
                                                                       
                                                                     })
  )
}



Provider_List_Mutate[["Referrals_MonthWeek"]] <- left_join(
  Provider_List_Mutate[["Referrals_MonthWeek"]],
  Provider_List[["RTT_Table_Pivot"]] %>% select(-Effective_Snapshot_Date),
  by = c(
    "Effective_Snapshot_Date_Year",
    "Effective_Snapshot_Date_Month"
  )
) 

Provider_List_Mutate[["Referrals_MonthWeek"]] %<>% map_at(
  c(
    "Completed.Pathways.For.Admitted.Patients",
    "Completed.Pathways.For.Non.Admitted.Patients",
    "Incomplete.Pathways",
    "Incomplete.Pathways.with.DTA",
    "RTT_Referrals"
  ),
  ~
    {
      pathway_vector <- .x
      
      for (idx in 1:length(pathway_vector)) {
        if (is.na(pathway_vector[idx]) &
            idx < max(which(!is.na(pathway_vector))) &
            idx > min(which(!is.na(pathway_vector)))) {
          pathway_vector[idx] <- pathway_vector[idx - 1]
        }
      }
      
      return(pathway_vector)
      
    }
) %>% as_tibble()

Provider_List_Mutate[["Referrals_MonthWeek"]] %<>% mutate_at(vars("Completed.Pathways.For.Admitted.Patients":"RTT_Referrals"),
                                                             ~ round(.*Propn, 0))

## Waiting List Wrangling ####

Provider_List_Mutate[["Pathways_Monthly"]] <- Provider_List_Mutate[["Referrals_MonthWeek"]] %>% ungroup() %>% group_by(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Month) %>% summarise(
  CompletedPathways_Admitted = sum(Completed.Pathways.For.Admitted.Patients),
  CompletedPathways_NonAdmitted = sum(Completed.Pathways.For.Non.Admitted.Patients),
  IncompletePathways = sum(Incomplete.Pathways),
  IncompletePathways_DTA = sum(Incomplete.Pathways.with.DTA),
  RTT_Referrals = sum(RTT_Referrals)
)

X2019_01 <- which(Provider_List_Mutate[["Pathways_Monthly"]]$Effective_Snapshot_Date_Year == 2019 & Provider_List_Mutate[["Pathways_Monthly"]]$Effective_Snapshot_Date_Month == 1)
Vector_Year <- Provider_List_Mutate[["Pathways_Monthly"]][X2019_01:nrow(Provider_List_Mutate[["Pathways_Monthly"]]),"Effective_Snapshot_Date_Year"] %>% pull()
Vector_Month <- Provider_List_Mutate[["Pathways_Monthly"]][X2019_01:nrow(Provider_List_Mutate[["Pathways_Monthly"]]),"Effective_Snapshot_Date_Month"] %>% pull()

walk2(Vector_Year,
      Vector_Month,
      ~ {
        df <- Provider_List_Mutate[["Pathways_Monthly"]]
        idx <- which(df$Effective_Snapshot_Date_Year == .x &
                       df$Effective_Snapshot_Date_Month == .y)
        
        Provider_List_Mutate[["Pathways_Monthly"]][[idx, "Temp"]] <<-
          df[[idx - 1, "IncompletePathways"]] -
          (df[[idx, "CompletedPathways_NonAdmitted"]] + df[[idx, "CompletedPathways_Admitted"]]) +
          df[[idx, "RTT_Referrals"]]
        
        Provider_List_Mutate[["Pathways_Monthly"]][[idx, "Leavers"]] <<- 
          df[[idx, "IncompletePathways"]] - Provider_List_Mutate[["Pathways_Monthly"]][[idx, "Temp"]]
        
        Provider_List_Mutate[["Pathways_Monthly"]][[idx, "Leavers_Inverse"]] <<- Provider_List_Mutate[["Pathways_Monthly"]][[idx, "Leavers"]]*-1
        
      }) ## There is definitely a more effective way to do this, but it works so..

## For unname

X2019_01_Week <-
  which(
    Provider_List_Mutate[["Referrals_MonthWeek"]]$Effective_Snapshot_Date_Year == 2019 &
      Provider_List_Mutate[["Referrals_MonthWeek"]]$Effective_Snapshot_Date_Month == 1 &
      Provider_List_Mutate[["Referrals_MonthWeek"]]$Effective_Snapshot_Date_Week == 1
  )
Vector_Year_Week <-
  Provider_List_Mutate[["Referrals_MonthWeek"]][X2019_01_Week:nrow(Provider_List_Mutate[["Referrals_MonthWeek"]]), "Effective_Snapshot_Date_Year"] %>% pull()
Vector_Month_Week <-
  Provider_List_Mutate[["Referrals_MonthWeek"]][X2019_01_Week:nrow(Provider_List_Mutate[["Referrals_MonthWeek"]]), "Effective_Snapshot_Date_Month"] %>% pull()
Vector_Week_Week <-   
  Provider_List_Mutate[["Referrals_MonthWeek"]][X2019_01_Week:nrow(Provider_List_Mutate[["Referrals_MonthWeek"]]), "Effective_Snapshot_Date_Week"] %>% pull()

pwalk(list(Vector_Year_Week,
           Vector_Month_Week,
           Vector_Week_Week),
      ~ {
        df <- Provider_List_Mutate[["Referrals_MonthWeek"]]
        idx <- which(df$Effective_Snapshot_Date_Year == ..1 &
                       df$Effective_Snapshot_Date_Month == ..2 &
                       df$Effective_Snapshot_Date_Week == ..3)
        
        idx_monthly <- which(Provider_List_Mutate[["Pathways_Monthly"]]$Effective_Snapshot_Date_Year == ..1 &
                               Provider_List_Mutate[["Pathways_Monthly"]]$Effective_Snapshot_Date_Month == ..2)
        
        Provider_List_Mutate[["Referrals_MonthWeek"]][[idx, "Unname"]] <<- 
          round(df[[idx, "Propn"]] *
                  Provider_List_Mutate[["Pathways_Monthly"]][[idx_monthly, "Leavers"]], 0)
        Provider_List_Mutate[["Referrals_MonthWeek"]][[idx, "Unname_Inverse"]] <<- Provider_List_Mutate[["Referrals_MonthWeek"]][[idx, "Unname"]]*-1
      }
)

## Create final table

Provider_List_Mutate[["RTT_PathwayAndReferrals_Complete"]] <-
  Provider_List_Mutate[["Referrals_MonthWeek"]] %>% ungroup() %>% group_by(Effective_Snapshot_Date) %>% summarise(
    CompletedPathways_Admitted = sum(Completed.Pathways.For.Admitted.Patients),
    CompletedPathways_NonAdmitted = sum(Completed.Pathways.For.Non.Admitted.Patients),
    IncompletePathways = sum(Incomplete.Pathways),
    IncompletePathways_DTA = sum(Incomplete.Pathways.with.DTA),
    RTT_Referrals = sum(RTT_Referrals),
    RTT_NotSeen = sum(Unname_Inverse)
  )

Provider_List_Mutate[["RTT_PathwayAndReferrals_Complete"]][["WaitingList"]] <- NA_real_
WL_idx <- which(Provider_List_Mutate[["RTT_PathwayAndReferrals_Complete"]]$Effective_Snapshot_Date == "2019-01")
Provider_List_Mutate[["RTT_PathwayAndReferrals_Complete"]][[WL_idx, "WaitingList"]] <-
  (Provider_List_Mutate[["Pathways_Monthly"]] %>%
     filter(Effective_Snapshot_Date_Year == 2018,
            Effective_Snapshot_Date_Month == 12) %>%
     pull(IncompletePathways))
Provider_List_Mutate[["RTT_PathwayAndReferrals_Complete"]][WL_idx:nrow(Provider_List_Mutate[["RTT_PathwayAndReferrals_Complete"]]),] %<>% 
  mutate(WaitingList =
           lag(
             first(WaitingList) - (
               cumsum(CompletedPathways_Admitted) + cumsum(CompletedPathways_NonAdmitted) + cumsum(RTT_NotSeen)
             ) + cumsum(RTT_Referrals),
             default = first(WaitingList)
           ))

Provider_List_Mutate[["Pathways_Monthly"]] <- NULL ## don't need anymore

## Check has cols ####

Check <- Provider_List_Mutate %>% map(~ (
  colnames(.x) %in% c("Effective_Snapshot_Date")
) %>% sum() == 1) %>% as_vector()

## Join ####

## Don't need ReferralsMonthWeek

if (all(Check)) {
  Binded <- reduce(Provider_List_Mutate[which(names(Provider_List_Mutate) != "Referrals_MonthWeek")], full_join, c("Effective_Snapshot_Date"))
  Binded <- Binded %>% select(-contains("Organisation_Code")) %>% filter(str_detect(Effective_Snapshot_Date, "2019|2020|2021"))
}

Binded %<>% arrange(Effective_Snapshot_Date)

## Fix broken rows in static cols ####

Binded %<>% fill(c("No_of_Operating_Theatres", contains("Number_Of_Beds"), "Consultant", "Medic_Sum", "Absence_PCT"), .direction = "down")

## Minor fix for GP Referrals ####

no_GP_referrals <- with(Binded, which(is.na(GP_Referrals) & str_detect(Effective_Snapshot_Date, "2019")))

if (length(no_GP_referrals) > 0) {
  Binded[no_GP_referrals, "GP_Referrals"] <- 0
  Binded[no_GP_referrals, "RTT_Referrals"] <- Binded[["RTT_Referrals"]][max(which(!is.na(Binded[["RTT_Referrals"]])))]
}

rm(no_GP_referrals)

## GP Referrals issue fix ####

Binded$GP_Referrals %<>% as.integer()
Binded %<>% mutate_at(vars("GP_Referrals"), funs(case_when(. > as.integer(RTT_Referrals) ~ as.integer(RTT_Referrals),
                                                           TRUE ~ .)))

## Create Derived columns ####

Binded %<>% mutate(OtherReferrals = RTT_Referrals-GP_Referrals,
                   PopNeed_GP = GP_Referrals,
                   PopNeed_Other = OtherReferrals,
                   Total_No_Beds = round(Number_Of_Beds_DAY+Number_Of_Beds_NIGHT, 1)
) %>% 
  select(-Number_Of_Beds_DAY, -Number_Of_Beds_NIGHT, -contains("NotSeen_PCT"), -contains("IncompletePathways"))

## Reorder ####

Binded %<>% select(Effective_Snapshot_Date, WaitingList, PopNeed_GP, PopNeed_Other, GP_Referrals, OtherReferrals, RTT_Referrals, CompletedPathways_Admitted, CompletedPathways_NonAdmitted, RTT_NotSeen, NotSeen_Recovered, NotSeen_Died, Total_No_Beds, Consultant, No_of_Operating_Theatres, Absence_PCT, Medic_Sum)

## Fill Yellow Columns ####

## rm OtherReferrals and Total_No_Beds

Binded %<>% fill(c("GP_Referrals", "OtherReferrals", "Total_No_Beds", "RTT_Referrals", "CompletedPathways_Admitted", "Consultant", "Medic_Sum", "CompletedPathways_NonAdmitted", "RTT_NotSeen"), .direction = "down")

## Prep Final Cols ####

Binded %<>% mutate(
  NoAdm_per_Bed = NA,
  NoAdm_per_Consultant = NA,
  NoAdm_per_Theatre = NA,
  NoSeen_per_Consultant = NA,
  Transfers = NA
)

for (colx in c("PopNeed_GP", "PopNeed_Other")) {
  iwalk(Binded[[colx]], ~ {
    if (is.na(.x)) {
      Relevant_Date <- Binded[[.y, "Effective_Snapshot_Date"]]
      New_Date <-
        paste0(as.integer(str_sub(Relevant_Date, 1, 4)) - 1,
               "-",
               str_sub(Relevant_Date, 6, 7))
      
      Binded[[.y, colx]] <<-
        Binded[[which(Binded$Effective_Snapshot_Date == New_Date), colx]]
      
    }
    
  })
}



init_week <- if (Specialty == 140) "2020-10" else "2020-12"

init_idx <- which(Binded$Effective_Snapshot_Date == init_week)

for (relevant_idx in init_idx:nrow(Binded)) {
  Relevant_Date <- Binded[[relevant_idx, "Effective_Snapshot_Date"]]
  
  New_Date <-
    paste0(as.integer(str_sub(Relevant_Date, 1, 4)) - 1,
           "-",
           str_sub(Relevant_Date, 6, 7))
  cat(New_Date, "\n")
  
  Binded[[relevant_idx, "PopNeed_GP"]] <-
    Binded[[which(Binded$Effective_Snapshot_Date == New_Date), "PopNeed_GP"]]
  Binded[[relevant_idx, "PopNeed_Other"]] <-
    Binded[[which(Binded$Effective_Snapshot_Date == New_Date), "PopNeed_Other"]]
  
}

rm(init_week)
rm(init_idx)

walk(which(is.na(Binded$WaitingList)), ~ {
  
  relevant_row <- Binded[.x-1,]
  
  Binded[[.x, "WaitingList"]] <<- with(relevant_row, WaitingList+GP_Referrals+OtherReferrals-(CompletedPathways_Admitted+CompletedPathways_NonAdmitted+NotSeen_Recovered+NotSeen_Died))
  
}
)

Binded %<>% select(Effective_Snapshot_Date, WaitingList, everything(), Transfers)

Binded %<>% add_column(Mortality_Rate = NA, .after = "Medic_Sum") %>% 
  add_column(Diagnostics = NA, .after = "NoAdm_per_Theatre") %>% 
  add_column(NoAdm_per_Diagnostic = NA, .after = "Diagnostics") %>% 
  add_column(NoSeen_per_Diagnostic = NA, .after = "NoSeen_per_Consultant") %>% 
  add_column(PCT_Recover = NA, .after = "NoSeen_per_Diagnostic")

## Add Diagnostics ####

cat("Adding in diagnostics data\n")

diag_specialty <- if (Specialty == "X01") "x01" else Specialty

tbl(
  con,
  build_sql(
    "SELECT    Organisation_Code 
    , CASE WHEN Diagnostic_ID IN ('12','13','14','15') THEN 'Endoscopy' 
    ELSE Diagnostic_Test_Name END AS Diagnostic_Test_Name
    , Effective_Snapshot_Date
    , Sum(WL_Tests_And_Procedures_Excl_Planned) as Tests   
    FROM [FD_UserDB].[central_midlands_csu_UserDB].[Diagnostic_Waits_And_Activity].[Activity_Mthly_Prov1]
    WHERE Effective_Snapshot_Date >'2018-12-31' AND Diagnostic_ID IN ('1','2','3','5','6','7','12','13','14','15')",
    "AND Organisation_Code IN ('",  sql(TrustCodesRegex), "')",
    " GROUP BY Organisation_Code 
    , CASE WHEN Diagnostic_ID IN ('12','13','14','15') THEN 'Endoscopy' 
    ELSE Diagnostic_Test_Name END
    , Effective_Snapshot_Date
    , WL_Tests_And_Procedures_Excl_Planned",
    con = con
  )
) %>% collect() -> test

DiagSpecSplits$Specialty %>% unique() -> diag_specialty_codes

# DiagSpecSplits <- DiagSpecSplits %>% filter(Percent != 0)


diag_df <- map2(purrr::set_names(DiagSpecSplits$Test, janitor::make_clean_names(paste0(DiagSpecSplits$Test, "_", DiagSpecSplits$Specialty))), DiagSpecSplits$Specialty, ~ {
  df <- test %>% filter(Diagnostic_Test_Name == .x)
  df$Tests <- round(df$Tests*(DiagSpecSplits %>% filter(Test == .x, Specialty == .y) %>% pull(Percent)), 0)
  
  if (.x == "Endoscopy") {
    df <- df %>% group_by(Effective_Snapshot_Date) %>% summarise(Organisation_Code = first(Organisation_Code), Diagnostic_Test_Name = first(Diagnostic_Test_Name), Tests = sum(Tests))
  }
  
  return(df)
})

diag_df[which(names(diag_df) %>% str_detect(diag_specialty))] -> diag_df

diag_df <- imap(diag_df, ~ {
  .x %>% rename(!!sym(.y) := "Tests") %>% select(-Diagnostic_Test_Name)
})

diag_df <- map(diag_df, ~ {

  use_colname <- colnames(.x)[3]

  .x %>% group_by(Effective_Snapshot_Date) %>% summarise(!!use_colname := sum(!!sym(use_colname))) %>% arrange(Effective_Snapshot_Date)

  })

diag_df <- diag_df %>% reduce(left_join, by = c("Effective_Snapshot_Date"))

diag_df %<>% rowwise %>% mutate_at(vars(contains(diag_specialty)),
                       function(x) if (all(is.na(x))) 0 else x) %>% ungroup()

diag_df <-
  diag_df %>%
  mutate_at("Effective_Snapshot_Date", as.Date)

diag_df$Effective_Snapshot_Date_Year <-
  year(diag_df$Effective_Snapshot_Date)
diag_df$Effective_Snapshot_Date_Month <-
  month(diag_df$Effective_Snapshot_Date)

proportions_rip <- Provider_List_Mutate[["Referrals_MonthWeek"]] %>% select(contains("Effective"), "Propn")

check_anti <- anti_join(diag_df %>% select(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Month), proportions_rip %>% select(Effective_Snapshot_Date_Year, Effective_Snapshot_Date_Month) %>% distinct())

if (nrow(check_anti) > 0) {
  
  walk2(check_anti$Effective_Snapshot_Date_Year, check_anti$Effective_Snapshot_Date_Month, ~ {
    
    temp <- dates_lookup_formula %>% filter(year == .x, month == .y) %>% select(-nrows, -days_in_month)
    temp %<>% rename(
      Effective_Snapshot_Date_Year = year,
      Effective_Snapshot_Date_Month = month,
      Effective_Snapshot_Date_Week = week,
      Propn = days_in_month_propn)
    
    proportions_rip <<- bind_rows(proportions_rip, temp)
  }
  )
}


diag_df <- left_join(
  diag_df,
  proportions_rip %>% select(-"Effective_Snapshot_Date"),
  by = c(
    "Effective_Snapshot_Date_Year",
    "Effective_Snapshot_Date_Month"
  )
)

diag_df %<>% mutate_at(vars(contains(diag_specialty)),
                       ~ round(.*Propn, 0))

diag_df <- mutate(diag_df, Effective_Snapshot_Date = paste0(
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

diag_df %<>% group_by(Effective_Snapshot_Date) %>% summarise_at(vars(contains(diag_specialty)), sum)

## Join with Binded again ####

Binded %<>% left_join(diag_df %>% select(contains(diag_specialty), "Effective_Snapshot_Date"), by = "Effective_Snapshot_Date")

Binded <- Binded %>% fill(contains(diag_specialty), .direction = "down")

## Diagnotics Derived ####

cat("Diagnostics derived from Admitted..\n")


walk(names(diag_df)[-1], ~ {
  
  adm_var <- paste0("NoAdm_", .x %>% str_remove_all(paste0("_", diag_specialty)))
  seen_var <- paste0("NoSeen_", .x %>% str_remove_all(paste0("_", diag_specialty)))
  Binded <<- Binded %>% mutate(
    !!adm_var := case_when(
      is.infinite(CompletedPathways_Admitted / UQ(rlang::sym(.x))) ~ 0,
      TRUE ~ CompletedPathways_Admitted / UQ(rlang::sym(.x))
    ), 
    !!seen_var := case_when(
      is.infinite(CompletedPathways_NonAdmitted / UQ(rlang::sym(.x))) ~ 0,
      TRUE ~ CompletedPathways_NonAdmitted / UQ(rlang::sym(.x))
    )
  )
  
}
)

adm_var <- paste0("NoAdm_", names(diag_df)[-1] %>% str_remove_all(paste0("_", diag_specialty)))
seen_var <- paste0("NoSeen_", names(diag_df)[-1] %>% str_remove_all(paste0("_", diag_specialty)))

diagnostics_columns <- pmap(list(names(diag_df)[-1], adm_var, seen_var), ~ c(..1, ..2, ..3)) %>% unlist()

Binded %<>% select(
  "Effective_Snapshot_Date",
  "WaitingList",
  "PopNeed_GP",
  "PopNeed_Other",
  "GP_Referrals",
  "OtherReferrals",
  "RTT_Referrals",
  "CompletedPathways_Admitted",
  "CompletedPathways_NonAdmitted",
  "RTT_NotSeen",
  "NotSeen_Recovered",
  "NotSeen_Died",
  "Total_No_Beds",
  "Consultant",
  "No_of_Operating_Theatres",
  "Absence_PCT",
  "Medic_Sum",
  "Mortality_Rate",
  "NoAdm_per_Bed",
  "NoAdm_per_Consultant",
  "NoAdm_per_Theatre",
  diagnostics_columns[diagnostics_columns %>% str_detect("endoscopy_")],
  "NoAdm_endoscopy",
  "NoSeen_per_Consultant",
  "NoSeen_endoscopy",
  "PCT_Recover",
  "Transfers",
  map(c("audiology", "cardiology_echocardiography", "computed_tomography", "dexa_scan", "magnetic_resonance_imaging", "non_obstetric_ultrasound"), ~ diagnostics_columns[diagnostics_columns %>% str_detect(.x)]) %>% unlist()
)

## New weeks ####

new_dates <- suppressWarnings(dates_lookup_formula[which(dates_lookup_formula$Effective_Snapshot_Date == "2020-27"):which(dates_lookup_formula$Effective_Snapshot_Date == "2021-13"),"Effective_Snapshot_Date"] %>% pull()) ## warning doesn't matter

walk(new_dates, function(date) {
  
  old_date <- paste0(date %>% str_sub(1, 4) %>% as.integer()-1, date %>% str_sub(5, 7))
  
  for (variable in setdiff(
    names(Binded),
    c(
      "Effective_Snapshot_Date",
      "WaitingList",
      "Total_No_Beds",
      "Consultant",
      "No_of_Operating_Theatres",
      "Absence_PCT",
      "Medic_Sum",
      "Mortality_Rate"
    )
  )) {
    Binded[which(Binded$Effective_Snapshot_Date == date),variable] <<- Binded[[which(Binded$Effective_Snapshot_Date == old_date),variable]]
  }
  
})

Binded[2:nrow(Binded),] %<>% mutate(WaitingList =
                                      lag(
                                        first(WaitingList) - (
                                          cumsum(CompletedPathways_Admitted) + cumsum(CompletedPathways_NonAdmitted) + cumsum(RTT_NotSeen)
                                        ) + cumsum(RTT_Referrals),
                                        default = first(WaitingList)
                                      ))

## Hot fix for values

Binded %<>% filter(Effective_Snapshot_Date != "2021-14")

Binded[which(Binded$Effective_Snapshot_Date %in% c("2021-12", "2021-13")),] %<>% mutate(GP_Referrals = PopNeed_GP,
                                                                                       OtherReferrals = PopNeed_Other)

Binded[which(Binded$Effective_Snapshot_Date == "2021-13"),"CompletedPathways_Admitted"] <- Binded[which(Binded$Effective_Snapshot_Date == "2019-13"),"CompletedPathways_Admitted"]
Binded[which(Binded$Effective_Snapshot_Date == "2021-13"),"CompletedPathways_NonAdmitted"] <- Binded[which(Binded$Effective_Snapshot_Date == "2019-13"),"CompletedPathways_NonAdmitted"]

## Last fill ####

Binded %<>% fill(c("Consultant", "No_of_Operating_Theatres", "Medic_Sum", "Total_No_Beds"), .direction = "down")

Binded %<>% mutate(PCT_Recover = NotSeen_Recovered/WaitingList,
                   Mortality_Rate = NotSeen_Died/WaitingList,  ## the waiting list gets manipulated so best to put this after at the end
                   NoAdm_per_Bed = round(
                     CompletedPathways_Admitted / Total_No_Beds,
                     2
                   ),
                   NoAdm_per_Consultant = round(CompletedPathways_Admitted / Consultant / (1 -
                                                                                             Absence_PCT), 2),
                   NoAdm_per_Theatre = round(CompletedPathways_Admitted / No_of_Operating_Theatres, 2),
                   NoSeen_per_Consultant = round(CompletedPathways_NonAdmitted / Consultant /
                                                   (1 - Absence_PCT), 2),
                   Transfers = RTT_NotSeen - (NotSeen_Recovered + NotSeen_Died)
                   )


## Time end ####

end <- Sys.time()

print(end-start)

return(Binded)

}
