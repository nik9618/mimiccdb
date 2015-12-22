datapath = '/Users/nik9618/Desktop/mimiccdb/database/total'
defpath = '/Users/nik9618/Desktop/mimiccdb/database/Definitions'

allFiles = ["A_CHARTDURATIONS","COMORBIDITY_SCORES","NOTEEVENTS","ADDITIVES","DELIVERIES","ICUSTAY_DETAIL","POE_MED","ADMISSIONS","DEMOGRAPHIC_DETAIL","ICUSTAYEVENTS","POE_ORDER","A_IODURATIONS","DEMOGRAPHICEVENTS","IOEVENTS","PROCEDUREEVENTS","A_MEDDURATIONS","D_PATIENTS","LABEVENTS","TOTALBALEVENTS","CENSUSEVENTS","DRGEVENTS","MEDEVENTS","CHARTEVENTS","ICD9","MICROBIOLOGYEVENTS"]
allDefs = ['D_CAREGIVERS','D_CODEDITEMS','D_LABITEMS','PARAMETER_MAPPING','D_CAREUNITS','D_DEMOGRAPHICITEMS','D_MEDITEMS','D_CHARTITEMS','D_IOITEMS']
allAttr = dict()

def init():
	allAttr["A_CHARTDURATIONS"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,ELEMID,STARTTIME,STARTREALTIME,ENDTIME,CUID,DURATION"
	allAttr["COMORBIDITY_SCORES"] = "SUBJECT_ID,HADM_ID,CATEGORY,CONGESTIVE_HEART_FAILURE,CARDIAC_ARRHYTHMIAS,VALVULAR_DISEASE,PULMONARY_CIRCULATION,PERIPHERAL_VASCULAR,HYPERTENSION,PARALYSIS,OTHER_NEUROLOGICAL,CHRONIC_PULMONARY,DIABETES_UNCOMPLICATED,DIABETES_COMPLICATED,HYPOTHYROIDISM,RENAL_FAILURE,LIVER_DISEASE,PEPTIC_ULCER,AIDS,LYMPHOMA,METASTATIC_CANCER,SOLID_TUMOR,RHEUMATOID_ARTHRITIS,COAGULOPATHY,OBESITY,WEIGHT_LOSS,FLUID_ELECTROLYTE,BLOOD_LOSS_ANEMIA,DEFICIENCY_ANEMIAS,ALCOHOL_ABUSE,DRUG_ABUSE,PSYCHOSES,DEPRESSION"
	#allAttr["ICUSTAY_DAYS"] = "ICUSTAY_ID,SUBJECT_ID,SEQ,BEGINTIME,ENDTIME,FIRST_DAY_FLG,LAST_DAY_FLG"
	allAttr["NOTEEVENTS"] = "SUBJECT_ID,HADM_ID,ICUSTAY_ID,ELEMID,CHARTTIME,REALTIME,CGID,CORRECTION,CUID,CATEGORY,TITLE,TEXT,EXAM_NAME,PATIENT_INFO"
	allAttr["ADDITIVES"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,IOITEMID,CHARTTIME,ELEMID,CGID,CUID,AMOUNT,DOSEUNITS,ROUTE"
	allAttr["DELIVERIES"] = "SUBJECT_ID,ICUSTAY_ID,IOITEMID,CHARTTIME,ELEMID,CGID,CUID,SITE,RATE,RATEUOM"

	allAttr["ICUSTAY_DETAIL"] = "ICUSTAY_ID,SUBJECT_ID,GENDER,DOB,DOD,EXPIRE_FLG,SUBJECT_ICUSTAY_TOTAL_NUM,SUBJECT_ICUSTAY_SEQ,HADM_ID,HOSPITAL_TOTAL_NUM,HOSPITAL_SEQ,HOSPITAL_FIRST_FLG,HOSPITAL_LAST_FLG,HOSPITAL_ADMIT_DT,HOSPITAL_DISCH_DT,HOSPITAL_LOS,HOSPITAL_EXPIRE_FLG,ICUSTAY_TOTAL_NUM,ICUSTAY_SEQ,ICUSTAY_FIRST_FLG,ICUSTAY_LAST_FLG,ICUSTAY_INTIME,ICUSTAY_OUTTIME,ICUSTAY_ADMIT_AGE,ICUSTAY_AGE_GROUP,ICUSTAY_LOS,ICUSTAY_EXPIRE_FLG,ICUSTAY_FIRST_CAREUNIT,ICUSTAY_LAST_CAREUNIT,ICUSTAY_FIRST_SERVICE,ICUSTAY_LAST_SERVICE,HEIGHT,WEIGHT_FIRST,WEIGHT_MIN,WEIGHT_MAX,SAPSI_FIRST,SAPSI_MIN,SAPSI_MAX,SOFA_FIRST,SOFA_MIN,SOFA_MAX,MATCHED_WAVEFORMS_NUM"
	allAttr["POE_MED"] = "POE_ID,DRUG_TYPE,DRUG_NAME,DRUG_NAME_GENERIC,PROD_STRENGTH,FORM_RX,DOSE_VAL_RX,DOSE_UNIT_RX,FORM_VAL_DISP,FORM_UNIT_DISP,DOSE_VAL_DISP,DOSE_UNIT_DISP,DOSE_RANGE_OVERRIDE"
	allAttr["ADMISSIONS"] = "HADM_ID,SUBJECT_ID,ADMIT_DT,DISCH_DT"
	allAttr["DEMOGRAPHIC_DETAIL"] = "SUBJECT_ID,HADM_ID,MARITAL_STATUS_ITEMID,MARITAL_STATUS_DESCR,ETHNICITY_ITEMID,ETHNICITY_DESCR,OVERALL_PAYOR_GROUP_ITEMID,OVERALL_PAYOR_GROUP_DESCR,RELIGION_ITEMID,RELIGION_DESCR,ADMISSION_TYPE_ITEMID,ADMISSION_TYPE_DESCR,ADMISSION_SOURCE_ITEMID,ADMISSION_SOURCE_DESCR"
	allAttr["ICUSTAYEVENTS"] = "ICUSTAY_ID,SUBJECT_ID,INTIME,OUTTIME,LOS,FIRST_CAREUNIT,LAST_CAREUNIT"
	allAttr["POE_ORDER"] = "POE_ID,SUBJECT_ID,HADM_ID,ICUSTAY_ID,START_DT,STOP_DT,ENTER_DT,MEDICATION,PROCEDURE_TYPE,STATUS,ROUTE,FREQUENCY,DISPENSE_SCHED,IV_FLUID,IV_RATE,INFUSION_TYPE,SLIDING_SCALE,DOSES_PER_24HRS,DURATION,DURATION_INTVL,EXPIRATION_VAL,EXPIRATION_UNIT,EXPIRATION_DT,LABEL_INSTR,ADDITIONAL_INSTR,MD_ADD_INSTR,RNURSE_ADD_INSTR"
	allAttr["A_IODURATIONS"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,ELEMID,STARTTIME,STARTREALTIME,ENDTIME,CUID,DURATION"
	allAttr["DEMOGRAPHICEVENTS"] = "SUBJECT_ID,HADM_ID,ITEMID"
	allAttr["IOEVENTS"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,CHARTTIME,ELEMID,ALTID,REALTIME,CGID,CUID,VOLUME,VOLUMEUOM,UNITSHUNG,UNITSHUNGUOM,NEWBOTTLE,STOPPED,ESTIMATE"
	allAttr["PROCEDUREEVENTS"] = "SUBJECT_ID,HADM_ID,ITEMID,SEQUENCE_NUM,PROC_DT"
	allAttr["A_MEDDURATIONS"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,ELEMID,STARTTIME,STARTREALTIME,ENDTIME,CUID,DURATION"
	allAttr["D_PATIENTS"] = "SUBJECT_ID,SEX,DOB,DOD,HOSPITAL_EXPIRE_FLG"
	allAttr["LABEVENTS"] = "SUBJECT_ID,HADM_ID,ICUSTAY_ID,ITEMID,CHARTTIME,VALUE,VALUENUM,FLAG,VALUEUOM"
	allAttr["TOTALBALEVENTS"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,CHARTTIME,ELEMID,REALTIME,CGID,CUID,PERVOLUME,CUMVOLUME,ACCUMPERIOD,APPROX,RESET,STOPPED"
	allAttr["CENSUSEVENTS"] = "CENSUS_ID,SUBJECT_ID,INTIME,OUTTIME,CAREUNIT,DESTCAREUNIT,DISCHSTATUS,LOS,ICUSTAY_ID"
	allAttr["DRGEVENTS"] = "SUBJECT_ID,HADM_ID,ITEMID,COST_WEIGHT"
	allAttr["MEDEVENTS"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,CHARTTIME,ELEMID,REALTIME,CGID,CUID,VOLUME,DOSE,DOSEUOM,SOLUTIONID,SOLVOLUME,SOLUNITS,ROUTE,STOPPED"
	allAttr["CHARTEVENTS"] = "SUBJECT_ID,ICUSTAY_ID,ITEMID,CHARTTIME,ELEMID,REALTIME,CGID,CUID,VALUE1,VALUE1NUM,VALUE1UOM,VALUE2,VALUE2NUM,VALUE2UOM,RESULTSTATUS,STOPPED"
	allAttr["ICD9"] = "SUBJECT_ID,HADM_ID,SEQUENCE,CODE,DESCRIPTION"
	allAttr["MICROBIOLOGYEVENTS"] = "SUBJECT_ID,HADM_ID,CHARTTIME,SPEC_ITEMID,ORG_ITEMID,ISOLATE_NUM,AB_ITEMID,DILUTION_AMOUNT,DILUTION_COMPARISON,INTERPRETATION"
