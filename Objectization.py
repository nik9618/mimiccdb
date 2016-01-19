
# coding: utf-8

# In[74]:

# DONE 
# join everything neatly
# make data admission oriented
# now im going to join them
# lets check integrity ?
# doest it has any link that not exists in D_ITEMS?
# sort inputevents
# date adjustment

# TODO
# check/test all 0/1
# test all + reconcile number of rows
# test sorting inputevents

# select+prune column to be perfect = remove unneccessary

# gen np package

# WORKING...


# NOTES
# PROCEDURES_ICD -- all icd9_code never eq to NULL
# but some of them are not exists when join
# 0 = exists 1 = not exist

# QUESTIONS
# 1. ie. transfer ? does it make sense to preserve time of the day? diff it with admission time doesn't preserve time of the day ...


# In[75]:

import psycopg2

try:
    # conn = psycopg2.connect("dbname='mimic' user='kanit' host='melady1.usc.edu' password='abcd6712'")
    conn = psycopg2.connect("dbname='mimic' user='kanit' host='localhost' password='abcd6712'")
except:
    print "I am unable to connect to the database"
print conn

cur = conn.cursor()
cur.execute('SELECT icd9_code FROM mimiciii.D_ICD_PROCEDURES')
icds = cur.fetchall()
tmp = []
for icd in icds:
    tmp.append(icd[0]);
icds = tmp;

cur = conn.cursor()
cur.execute('SELECT itemid FROM mimiciii.D_ITEMS')
ditems = cur.fetchall()
tmp = []
for item in ditems:
    tmp.append(item[0]);
ditems = tmp;

cur = conn.cursor()
cur.execute('SELECT itemid FROM mimiciii.D_LABITEMS')
dlabitems = cur.fetchall()
tmp = []
for item in dlabitems:
    tmp.append(item[0]);
dlabitems = tmp;

cur = conn.cursor()
cur.execute('SELECT cgid FROM mimiciii.CAREGIVERS')
caregivers = cur.fetchall()
tmp = []
for cg in caregivers:
    tmp.append(cg[0]);
caregivers = tmp;


# In[76]:

PATIENTS = [];
cur = conn.cursor()
cur.execute("""SELECT * FROM mimiciii.PATIENTS ORDER BY subject_id limit 100""")

rows = cur.fetchall()

for r in rows :
    PATIENTS.append(r)

print "DISTINCT PATIENTS : " + str(len(PATIENTS))


# In[98]:

PATIENTS_ADM = [];
ADMISSIONS = [];
CALLOUTS = [];
ICD9 = [];
DRGCODES = [];
ICUSTAYS = [];
SERVICES = [];
TRANSFERS = [];
CPTEVENTS = [];
PROCEDURES_ICD = [];
PRESCRIPTIONS = [];

ct = 0
for i in PATIENTS:
    i=list(i)
    
    ct+=1;
    if(ct%3000 == 0):
        print('.'),
    
    cur = conn.cursor()
    sql = 'SELECT * FROM mimiciii.ADMISSIONS WHERE subject_id='+str(i[1])+' ORDER BY row_id'
#     print sql
    cur.execute(sql)
    rows = cur.fetchall()
    
    obj = []
    for r in rows:
        r = list(r)
        
        # PATIENTS
        PATIENTS_ADM.append(i)
#         print i
#         print "..."
        
        # ADMISSIONS
        ADMISSIONS.append(r)
#         print r
#         print "..."

        # CALLOUT
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.CALLOUT WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY row_id')
        call_rows = cur.fetchall()
        crs = []
        for cr in call_rows:
            crs.append(list(cr))
        CALLOUTS.append(crs)
#         print list(call_rows)
#         print "..."
        
        # ICD9
        cur = conn.cursor()
        cur.execute('SELECT icd9_code FROM mimiciii.DIAGNOSES_ICD WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY seq_num')
        icd9s = cur.fetchall() 
        list_icd9 = []
        for icd9 in icd9s:
            list_icd9.append(icd9[0]);
        ICD9.append(list_icd9)
#         print list_icd9
#         print '...'
        
        # DRGCODES
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.DRGCODES WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY row_id')
        drgs = cur.fetchall()
        list_drgs = []
        for drg in drgs:
            list_drgs.append(list(drg));
        DRGCODES.append(list_drgs)
#         print list_drgs
#         print '...'
        
        # ICUSTAYS
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.ICUSTAYS WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY row_id')
        icustays = cur.fetchall()
        list_stays = []
        for icustay in icustays:
            list_stays.append(list(icustay));
        ICUSTAYS.append(list_stays)
#         print list_stays
#         print '...'

        # SERVICES
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.SERVICES WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY row_id')
        services = cur.fetchall()
        list_services = []
        for service in services:
            list_services.append(list(service));
        SERVICES.append(list_services)
#         print list_services
#         print '...'
        
        # TRANSFER
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.TRANSFERS WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY row_id')
        trans = cur.fetchall()
        list_trans = []
        for tran in trans:
            list_trans.append(list(tran));
        TRANSFERS.append(list_trans)        
#         print list_trans
#         print '...'
        
    
        # CPTEVENTS
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.CPTEVENTS WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY row_id')
        cpts = cur.fetchall()
        list_cpts = []
        for cpt in cpts:
            list_cpts.append(list(cpt));
        CPTEVENTS.append(list_cpts)   
#         print list_cpts
#         print '...'
    
        # PRESCRIPTIONS
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.PRESCRIPTIONS WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY row_id')
        pres = cur.fetchall()
        list_pres = []
        for pre in pres:
            list_pres.append(list(pre));
        PRESCRIPTIONS.append(list_pres)
#         print list_pres
#         print '...'
    
        # PROCEDURES_ICD
        cur = conn.cursor()
        cur.execute('SELECT * FROM mimiciii.PROCEDURES_ICD as p LEFT OUTER JOIN mimiciii.D_ICD_PROCEDURES as d ON d.ICD9_CODE = p.ICD9_CODE WHERE hadm_id='+str(r[2])+' AND subject_id='+str(r[1])+' ORDER BY seq_num')
        pros = cur.fetchall()
        list_pros = []
        for pro in pros:
            p = list(pro);
            if(p[4] in icds or p[4]==None):
                p.append(0);
            else:
                p.append(1);
            list_pros.append(p);
        PROCEDURES_ICD.append(list_pros)
#         print list_pros
#         print '...' 
    


# In[37]:

CHARTEVENTS = []

for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    chartadm=[]
    aid = ADMISSIONS[j][2]
    cur = conn.cursor()
    cur.execute('SELECT * FROM mimiciii.CHARTEVENTS as c LEFT OUTER JOIN mimiciii.D_ITEMS as d ON c.itemid = d.itemid LEFT OUTER JOIN mimiciii.CAREGIVERS as cg ON cg.cgid = c.cgid WHERE c.hadm_id='+str(aid)+' AND c.subject_id='+str(pid)+' ORDER BY c.row_id')
    charts = cur.fetchall()
    for c in charts:
        clist = list(c);
        if(c[4] in ditems or c[4] == None):
            clist.append(0)
        else:
            clist.append(1)
        if(c[7] in caregivers or c[7] == None):
            clist.append(0)
        else:
            clist.append(1)
            
        chartadm.append(clist)
    
    CHARTEVENTS.append(chartadm);

print 'end'


# In[109]:

DATETIMEEVENTS = []
    
for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    dtadm=[]
    aid = ADMISSIONS[j][2] 
        
    cur = conn.cursor()
    cur.execute('SELECT * FROM mimiciii.DATETIMEEVENTS as dt LEFT OUTER JOIN mimiciii.D_ITEMS as d ON dt.itemid = d.itemid LEFT OUTER JOIN mimiciii.CAREGIVERS as cg ON cg.cgid = dt.cgid WHERE dt.hadm_id='+str(aid)+' AND dt.subject_id='+str(pid)+' ORDER BY dt.row_id')
    dts = cur.fetchall()

    for dt in dts:
        dtlist = list(dt)
        if(dt[4] in ditems or dt[4] == None):
            dtlist.append(0)
        else:
            dtlist.append(1)
        if(dt[7] in caregivers or dt[7] == None):
            dtlist.append(0)
        else:
            dtlist.append(1)
        dtadm.append(dtlist)

    DATETIMEEVENTS.append(dtadm);
#     print dtadm
#     print '.'
    
print 'end'


# In[42]:

MICROBIOLOGYEVENTS = []
    
for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    bioadm=[]
    aid = ADMISSIONS[j][2] 
        
    cur = conn.cursor()
    cur.execute('SELECT * FROM mimiciii.MICROBIOLOGYEVENTS as b LEFT OUTER JOIN mimiciii.D_ITEMS as d1 ON b.spec_itemid = d1.itemid LEFT OUTER JOIN mimiciii.D_ITEMS as d2 ON b.org_itemid = d2.itemid LEFT OUTER JOIN mimiciii.D_ITEMS as d3 ON b.ab_itemid = d3.itemid WHERE b.hadm_id='+str(aid)+' AND b.subject_id='+str(pid)+' ORDER BY b.row_id')
    bios = cur.fetchall()
    for bio in bios:
        blist = list(bio)
        
        if(dt[5] in ditems or dt[5] == None):
            blist.append(0)
        else:
            blist.append(1)
        
        if(dt[7] in ditems or dt[7] == None):
            blist.append(0)
        else:
            blist.append(1)
            
        if(dt[10] in ditems or dt[10] == None):
            blist.append(0)
        else:
            blist.append(1)
            
        bioadm.append(blist)
    
    MICROBIOLOGYEVENTS.append(bioadm);

print 'end'


# In[43]:

NOTEEVENTS = []

for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    noteadm=[]
    aid = ADMISSIONS[j][2] 
    
    cur = conn.cursor()
    cur.execute('SELECT * FROM mimiciii.NOTEEVENTS as n LEFT OUTER JOIN mimiciii.CAREGIVERS as cg ON n.cgid = cg.cgid WHERE n.hadm_id='+str(aid)+' AND n.subject_id='+str(pid)+' ORDER BY n.row_id')
    notes = cur.fetchall()
    for note in notes:
        nlist = list(note);
        if(note[8] in caregivers or note[10] == None):
            nlist.append(0)
        else:
            nlist.append(1)
            
        noteadm.append(nlist)
    
    NOTEEVENTS.append(noteadm);
    
print 'end'


# In[44]:

OUTPUTEVENTS = []
for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    outadm=[]
    aid = ADMISSIONS[j][2] 
    
    cur = conn.cursor()
    cur.execute('SELECT * FROM mimiciii.OUTPUTEVENTS as o LEFT OUTER JOIN mimiciii.CAREGIVERS as cg ON o.cgid = cg.cgid LEFT OUTER JOIN mimiciii.D_ITEMS as d ON o.itemid = d.itemid WHERE o.hadm_id='+str(aid)+' AND o.subject_id='+str(pid)+' ORDER BY o.row_id')
    outs = cur.fetchall()
    
    for out in outs:
        olist = list(out);
        
        if(out[5] in ditems or out[5] == None):
            olist.append(0)
        else:
            olist.append(1)
            
        if(out[9] in caregivers or out[9] == None):
            olist.append(0)
        else:
            olist.append(1)
            
        outadm.append(olist)

    OUTPUTEVENTS.append(outadm);
    
print 'end'


# In[50]:

LABEVENTS = []
for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    labadm=[]
    aid = ADMISSIONS[j][2] 
        
    cur = conn.cursor()
    cur.execute('SELECT * FROM mimiciii.LABEVENTS as l LEFT OUTER JOIN mimiciii.D_LABITEMS as d ON l.itemid = d.itemid WHERE l.hadm_id='+str(aid)+' AND l.subject_id='+str(pid)+' ORDER BY l.row_id')
    labs = cur.fetchall()
    
    for lab in labs:
        llist = list(lab);
        
        if(out[3] in dlabitems or out[3] == None):
            llist.append(0)
        else:
            llist.append(1)
            
        labadm.append(llist)
        
    LABEVENTS.append(labadm);
    
print 'end'


# In[53]:

PROCEDUREEVENTS_MV = []
for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    promvadm=[]
    aid = ADMISSIONS[j][2] 
    
    cur = conn.cursor()
    cur.execute('SELECT * FROM mimiciii.PROCEDUREEVENTS_MV as p LEFT OUTER JOIN mimiciii.D_ITEMS as d ON p.itemid = d.itemid LEFT OUTER JOIN mimiciii.CAREGIVERS as cg ON cg.cgid = p.cgid WHERE p.hadm_id='+str(aid)+' AND p.subject_id='+str(pid)+' ORDER BY p.row_id')
    promvs = cur.fetchall()
    
    for promv in promvs:
        plist = list(promv);
        
        if(promv[6] in ditems or promv[6] == None):
            plist.append(0)
        else:
            plist.append(1)
            
        if(promv[12] in caregivers or promv[12] == None):
            plist.append(0)
        else:
            plist.append(1)
            
        promvadm.append(plist)
        
    PROCEDUREEVENTS_MV.append(promvadm);

print 'end'


# In[63]:

INPUTEVENTS = []
for j in range(len(ADMISSIONS)):
    pid = PATIENTS_ADM[j][1]
    inputadm = []
    aid = ADMISSIONS[j][2]     
        
    cur = conn.cursor()
    cur.execute('SELECT \'CV\',cv.SUBJECT_ID,cv.HADM_ID,cv.ICUSTAY_ID,NULL,NULL,cv.ITEMID,cv.AMOUNT,cv.AMOUNTUOM,cv.RATE,cv.RATEUOM,cv.STORETIME,cv.CGID,cv.ORDERID,cv.LINKORDERID,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,cv.ORIGINALAMOUNT,cv.ORIGINALRATE,cv.ORIGINALROUTE,cv.ORIGINALSITE,cv.STOPPED,cv.NEWBOTTLE,cv.ORIGINALRATEUOM,cv.ORIGINALAMOUNTUOM,cv.CHARTTIME FROM mimiciii.INPUTEVENTS_CV as cv LEFT OUTER JOIN mimiciii.D_ITEMS as d ON d.itemid = cv.itemid LEFT OUTER JOIN mimiciii.CAREGIVERS as c ON c.cgid = cv.cgid WHERE cv.hadm_id='+str(aid)+' AND cv.subject_id='+str(pid)+' ORDER BY cv.row_id')
    incvs = cur.fetchall()
    lcvs = len(incvs)
    for incv in incvs:
        ilist = list(incv);
        
        if(incv[5] in ditems or incv[5] == None):
            ilist.append(0)
        else:
            ilist.append(1)
            
        if(incv[11] in caregivers or incv[11] == None):
            ilist.append(0)
        else:
            ilist.append(1)
            
        inputadm.append(ilist)
    
    cur = conn.cursor()
    cur.execute('SELECT \'MV\',mv.SUBJECT_ID,mv.HADM_ID,mv.ICUSTAY_ID,mv.STARTTIME,mv.ENDTIME,mv.ITEMID,mv.AMOUNT,mv.AMOUNTUOM,mv.RATE,mv.RATEUOM,mv.STORETIME,mv.CGID,mv.ORDERID,mv.LINKORDERID,mv.ORDERCATEGORYNAME,mv.SECONDARYORDERCATEGORYNAME,mv.ORDERCOMPONENTTYPEDESCRIPTION,mv.ORDERCATEGORYDESCRIPTION,mv.PATIENTWEIGHT,mv.TOTALAMOUNT,mv.TOTALAMOUNTUOM,mv.ISOPENBAG,mv.CONTINUEINNEXTDEPT,mv.CANCELREASON,mv.STATUSDESCRIPTION,mv.COMMENTS_EDITEDBY,mv.COMMENTS_CANCELEDBY,mv.COMMENTS_DATE,mv.ORIGINALAMOUNT,mv.ORIGINALRATE,NULL,NULL,NULL,NULL,NULL,NULL,NULL FROM mimiciii.INPUTEVENTS_MV as mv LEFT OUTER JOIN mimiciii.D_ITEMS as d ON d.itemid = mv.itemid LEFT OUTER JOIN mimiciii.CAREGIVERS as c ON c.cgid = mv.cgid WHERE mv.hadm_id='+str(aid)+' AND mv.subject_id='+str(pid)+' ORDER BY mv.row_id')
    inmvs = cur.fetchall()
    lmvs = len(inmvs)
    for inmv in inmvs:
        ilist = list(inmv);
        
        if(inmv[5] in ditems or inmv[5] == None):
            ilist.append(0)
        else:
            ilist.append(1)
            
        if(inmv[11] in caregivers or inmv[11] == None):
            ilist.append(0)
        else:
            ilist.append(1)
            
        inputadm.append(ilist)
    
    #sort
    def compare(item1, item2):
        if item1[4] < item2[4]:
            return -1
        elif item1[4] > item2[4]:
            return 1
        else:
            return 0
    inputadm = sorted(inputadm,cmp=compare)
    INPUTEVENTS.append(inputadm);

print 'end'
    


# In[116]:

# date adjustment 
def append(obj, i, j, dob):
    if(obj[i][j] == None):
        obj[i].append(None)
        obj[i].append(None)
    else:
        x=(obj[i][j] - dob)
        obj[i].append(x.days);
        obj[i].append(x.seconds);
    
for i in range(len(ADMISSIONS)):
    dob = PATIENTS_ADM[i][3]
    amt = ADMISSIONS[i][3]
    
    # PATIENTS_ADM
    append(PATIENTS_ADM,i,4,dob)
    append(PATIENTS_ADM,i,5,dob)
    append(PATIENTS_ADM,i,6,dob)
    
    # ADMISSIONS = [];
    append(ADMISSIONS,i,3,dob)
    append(ADMISSIONS,i,4,amt)
    append(ADMISSIONS,i,14,amt)
    append(ADMISSIONS,i,15,amt)
    
    # CALLOUTS = [];
    for j in range(len(CALLOUTS[i])):
        append(CALLOUTS[i],j,18,amt)
        append(CALLOUTS[i],j,19,amt)
        append(CALLOUTS[i],j,20,amt)
        append(CALLOUTS[i],j,21,amt)
        append(CALLOUTS[i],j,22,amt)
        append(CALLOUTS[i],j,23,amt)

    # ICD9 = [];
    # DRGCODES = [];
    # ICUSTAYS = [];
    for j in range(len(ICUSTAYS[i])):
        append(ICUSTAYS[i],j,9,dob)
        append(ICUSTAYS[i],j,10,amt)

    # SERVICES = [];
    for j in range(len(SERVICES[i])):
        append(SERVICES[i],j,3,amt)
    
    # TRANSFERS = [];
    for j in range(len(TRANSFERS[i])):
        append(TRANSFERS[i],j,10,amt)
        append(TRANSFERS[i],j,11,amt)

    # CPTEVENTS = [];
    # PROCEDURES_ICD = [];
    # PRESCRIPTIONS = [];
    for j in range(len(PRESCRIPTIONS[i])):
        append(PRESCRIPTIONS[i],j,4,amt)
        append(PRESCRIPTIONS[i],j,5,amt)
      
    # PROCEDUREEVENTS_MV
    for j in range(len(PROCEDUREEVENTS_MV[i])):
        append(PROCEDUREEVENTS_MV[i],j,4,amt)
        append(PROCEDUREEVENTS_MV[i],j,5,amt)
        append(PROCEDUREEVENTS_MV[i],j,11,amt)
        append(PROCEDUREEVENTS_MV[i],j,24,amt)
    
    # LABEVENTS
    for j in range(len(LABEVENTS[i])):
        append(LABEVENTS[i],j,4,amt)
    
    # OUTPUTEVENTS    
    for j in range(len(OUTPUTEVENTS[i])):
        append(OUTPUTEVENTS[i],j,4,amt)
        append(OUTPUTEVENTS[i],j,8,amt)
    
    # NOTEEVENTS
    for j in range(len(NOTEEVENTS[i])):
        append(NOTEEVENTS[i],j,3,amt)
        append(NOTEEVENTS[i],j,4,amt)
        append(NOTEEVENTS[i],j,5,amt)
    
    # MICROBIOLOGYEVENTS
    for j in range(len(MICROBIOLOGYEVENTS[i])):
        append(MICROBIOLOGYEVENTS[i],j,3,amt)
        append(MICROBIOLOGYEVENTS[i],j,4,amt)
        
    # DATETIMEEVENTS
    for j in range(len(DATETIMEEVENTS[i])):
        append(DATETIMEEVENTS[i],j,5,amt)
        append(DATETIMEEVENTS[i],j,6,amt)
        append(DATETIMEEVENTS[i],j,8,amt)
    
    # CHARTEVENTS
    for j in range(len(CHARTEVENTS[i])):
        append(CHARTEVENTS[i],j,5,amt)
        append(CHARTEVENTS[i],j,6,amt)

    # INPUTEVENTS
    for j in range(len(INPUTEVENTS[i])):
        append(INPUTEVENTS[i],j,4,amt)
        append(INPUTEVENTS[i],j,5,amt)
        append(INPUTEVENTS[i],j,11,amt)
        append(INPUTEVENTS[i],j,28,amt)
        append(INPUTEVENTS[i],j,37,amt)   

print 'end'


# In[ ]:




# In[ ]:




# In[ ]:



