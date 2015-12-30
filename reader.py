from config import *
import re
import csv
import os
import sys
import numpy as np
import json
from datetime import datetime
from collections import OrderedDict

def readDef():
	definitions = {}
	for d in allDefs:
		with open(defpath+"/"+d+".txt") as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			first = True
			header = [];
			obj = {}
			for r in reader:
				if first :
					first = False
					header = r
				else:
					obj[r[0]] = {}
					for i in range(len(header)):
						obj[r[0]][header[i]] = r[i]
		definitions[d] = obj

	return definitions

def checkHeader(key,val):
	if val == (allAttr[key]+"\n") :
		return True 
	return False

def t(d,k,i=0):
	return d['data'][i][d['header'].index(k)]

def pathFile(id,key):
	return ("%s/%.5d/%s-%.5d.txt") % (datapath, id, key,id)		

def toDate(s):
	if(s == ''):
		s = None
	else:
		s = str(datetime.strptime(s, '%Y-%m-%d'))
		pass;
	return s

def toDateConv(s):
	if(s == ''):
		s = None
	else:
		assert dobDate != None
		diff = datetime.strptime(s, '%Y-%m-%d')-dobDate
		days, seconds = diff.days, diff.seconds
		s = days
		pass;
	return s

def toDatetime(s):
	if(s == ''):
		s = None
	else:
		assert s[len(s)-4:len(s)] == ' EST'
		s = str(datetime.strptime(s[0:-4], '%Y-%m-%d %H:%M:%S'))
		pass;
	return s

def toDatetimeConv(s):
	if(s == ''):
		s = None
	else:
		assert s[len(s)-4:len(s)] == ' EST'
		diff = datetime.strptime(s[0:-4], '%Y-%m-%d %H:%M:%S') - dobDate
		# minutes = diff.days *24*60 + diff.seconds / 60 
		return diff.days,diff.seconds/60.
		pass;
	return s

def toInt(str):
	if(str == None):
		return str
	elif(str == ''):
		str = None
	else:
		str = int(str)
	return str


def toFloat(str):
	if(str == None):
		return str
	elif(str == ''):
		str = None
	else:
		str = float(str)
	return str

def changeType(obj,types):
	if(obj==None):
		return
	
	for t in types.keys():
		if(types[t] == 'int'):
			obj[t] = toInt(obj[t])
		if(types[t] == 'float'):
			obj[t] = toFloat(obj[t])
		if(types[t] == 'date'):
			tmp = obj[t]
			obj[t] = toDate(tmp)
			obj[t+"_diffdays"] = toDateConv(tmp)
			pass;
		if(types[t] == 'datetime'):
			tmp = obj[t] 
			obj[t] = toDatetime(tmp)
			x = toDatetimeConv(tmp)
			if(x != None):
				obj[t+"_diff"] = {'days':x[0] , 'mins':x[1]}
			pass;

def joinEngine(header,data,f,stname, bydef):
	lenhd = len(header)
	toname = stname+"_J"

	for i in range(lenhd):
		if(stname == header[i]):
			header.append(toname)
			allAttr[f] += ','+toname
			for j in range(len(data)):
				if(data[j][i] in jdata[bydef]):
					data[j].append(jdata[bydef][data[j][i]])
				else:
					data[j].append(None)
	return header,data

def joindata(id,header, data, f):
	if(header == None):
		return header,data

	if(f == 'A_CHARTDURATIONS'):
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'ITEMID', 'D_CHARTITEMS')

	if(f == 'COMORBIDITY_SCORES'):
		pass;
	
	# if(f == 'ICUSTAY_DAYS'):
	# 	header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
	
	if(f == 'NOTEEVENTS'):
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'CGID', 'D_CAREGIVERS')
	
	if(f == 'ADDITIVES'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_MEDITEMS')
		header,data = joinEngine(header,data,f,'IOITEMID', 'D_IOITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'CGID', 'D_CAREGIVERS')
	
	if(f == 'DELIVERIES'):
		header,data = joinEngine(header,data,f,'IOITEMID', 'D_IOITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'CGID', 'D_CAREGIVERS')
	
	if(f == 'ICUSTAY_DETAIL'):
		pass;
	
	if(f == 'POE_MED'):
		#########
		pass;
	
	if(f == 'POE_ORDER'):
		med = []
		with open(pathFile(id,'POE_MED')) as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			first = True
			for r in reader:
				if first :
					first = False
				else:
					med.append(r)

		for i in range(len(data)):
			medMain = []
			for j in range(len(med)):
				if(data[i][0] == med[j][0]):
					attr = allAttr['POE_MED'].split(",")
					m = {}
					for k in range(len(attr)):
						m[attr[k]] = med[j][k]
					medMain.append(m)
			
			data[i].append(medMain)

		header.append('POE_MED')
		allAttr['POE_ORDER']+=",POE_MED"
		pass;

	if(f == 'ADMISSIONS'):
		pass;
	
	if(f == 'DEMOGRAPHIC_DETAIL'):
		pass;
	
	if(f == 'ICUSTAYEVENTS'):
		header,data = joinEngine(header,data,f,'FIRST_CAREUNIT', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'LAST_CAREUNIT', 'D_CAREUNITS')	
		pass;
	
	if(f == 'A_IODURATIONS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_IOITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
	
	if(f == 'DEMOGRAPHICEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_DEMOGRAPHICITEMS')
	
	if(f == 'IOEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_IOITEMS')
		header,data = joinEngine(header,data,f,'ALTID', 'D_IOITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'CGID', 'D_CAREGIVERS')
	
	if(f == 'PROCEDUREEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_CODEDITEMS')
	
	if(f == 'A_MEDDURATIONS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_MEDITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
	
	if(f == 'D_PATIENTS'):
		pass;
	
	if(f == 'LABEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_LABITEMS')
	
	if(f == 'TOTALBALEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_IOITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'CGID', 'D_CAREGIVERS')

	if(f == 'CENSUSEVENTS'):
		header,data = joinEngine(header,data,f,'CAREUNIT', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'DESTCAREUNIT', 'D_CAREUNITS')
	
	if(f == 'DRGEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_CODEDITEMS')
	
	if(f == 'MEDEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_MEDITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'CGID', 'D_CAREGIVERS')

	if(f == 'CHARTEVENTS'):
		header,data = joinEngine(header,data,f,'ITEMID', 'D_CHARTITEMS')
		header,data = joinEngine(header,data,f,'CUID', 'D_CAREUNITS')
		header,data = joinEngine(header,data,f,'CGID', 'D_CAREGIVERS')
	
	if(f == 'ICD9'):
		# header,data = joinEngine(header,data,f,'CODE', '')
		pass;
	
	if(f == 'MICROBIOLOGYEVENTS'):
		header,data = joinEngine(header,data,f,'AB_ITEMID', 'D_CODEDITEMS')
		header,data = joinEngine(header,data,f,'ORG_ITEMID', 'D_CODEDITEMS')
		header,data = joinEngine(header,data,f,'SPEC_ITEMID', 'D_CODEDITEMS')

	return header,data

def readPatient(id):
	path = (datapath+"/%.5d") % id
	if (not os.path.isdir( path )):
		return None

	init();
	patient = {};
	for f in allFiles:
		header = None
		data = []
		if (os.path.isfile(pathFile(id,f))) : 
			with open(pathFile(id,f)) as csvfile:
				reader = csv.reader(csvfile, delimiter=',', quotechar='"')
				nrow = sum(1 for row in reader)
				if(nrow>0) : 
					header = []
					data = []
			
			if(nrow > 0):
				with open(pathFile(id,f)) as fi:
					s = fi.readline()
					assert checkHeader(f,s)

				with open(pathFile(id,f)) as csvfile:
					reader = csv.reader(csvfile, delimiter=',', quotechar='"')
					first = True
					for r in reader:
						if first :
							first = False
							header = r
						else:
							data.append(r)

		patient[f]=dict()
		header,data = joindata(id,header,data,f)
		patient[f]['header'] = header
		patient[f]['data'] = data

		if(header != None and len(data)>0):
			for d in data:
				assert len(header) == len(d)

	return patient

dobDate = None
def patientObject(id):
	
	EMBED_DRGEVENTS_MICROBIOLOGYEVENTS_PROCEDUREEVENTS = True
	EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL = True
	EMBED_EVENTS = True

	data = readPatient(id);
	if(data == None) :
		print "Folder not found : "+ str(id)
		return 
	obj = dict();

	# -- DPATIENTS
	obj['SUBJECT_ID'] = t(data['D_PATIENTS'],'SUBJECT_ID')
	obj['SEX'] = t(data['D_PATIENTS'],'SEX')
	obj['DOB'] = t(data['D_PATIENTS'],'DOB')
	assert obj['DOB'] != ''
	global dobDate
	dobDate = datetime.strptime(obj['DOB'], '%Y-%m-%d')
	# print "DOB : "+ str(dobDate)
	obj['DOD'] = t(data['D_PATIENTS'],'DOD')
	obj['HOSPITAL_EXPIRE_FLG'] = t(data['D_PATIENTS'],'HOSPITAL_EXPIRE_FLG')

	# -- Declares
	obj['ELSE'] = {}
	obj['ELSE']['DRGEVENTS'] = []
	obj['ELSE']['MICROBIOLOGYEVENTS'] = []
	obj['ELSE']['PROCEDUREEVENTS'] = []
	
	# -- dont activate them, will be initialized later
	# obj['ELSE']['ICUSTAYEVENTS'] = [] 
	# obj['ELSE']['ICUSTAY_DETAIL'] = []
	# obj['ELSE']['DEMOGRAPHICEVENTS'] = []
	# obj['ELSE']['DEMOGRAPHIC_DETAIL'] = []

	obj['ELSE']['IOEVENTS'] = []
	obj['ELSE']['LABEVENTS'] = []
	obj['ELSE']['MEDEVENTS'] = []
	obj['ELSE']['NOTEEVENTS'] = []
	obj['ELSE']['TOTALBALEVENTS'] = []
	obj['ELSE']['A_CHARTDURATIONS'] = []
	obj['ELSE']['A_IODURATIONS'] = []
	obj['ELSE']['A_MEDDURATIONS'] = []
	obj['ELSE']['ADDITIVES'] = [] 
	obj['ELSE']['CENSUSEVENTS'] = [] 
	obj['ELSE']['CHARTEVENTS'] = [] 
	obj['ELSE']['DELIVERIES'] = []
	obj['ELSE']['POE_ORDER'] =[]


	# -- ADMISSIONS
	obj['ADMISSIONS'] = dict();

	if(len(data['ADMISSIONS']['data']) > 0 ):
		for i in range(len(data['ADMISSIONS']['data'])):
			hadm_id = t(data['ADMISSIONS'],'HADM_ID',i)
			obj['ADMISSIONS'][hadm_id] = {
				'HADM_ID':hadm_id,
				'ADMIT_DT':t(data['ADMISSIONS'],'ADMIT_DT',i),
				'DISCH_DT':t(data['ADMISSIONS'],'DISCH_DT',i)
			}

		for k in obj['ADMISSIONS'].keys():
			obj['ADMISSIONS'][k]['ICD9'] = []
			obj['ADMISSIONS'][k]['COMORBIDITY_SCORES'] = None
		
		# -- INTO admission BY hadm_id
		# -- ICD9, COMORBIDITY_SCORES
		focuses = ['ICD9']
		for focus in focuses:
			attr = allAttr[focus].split(",")
			for i in range(len(data[focus]['data'])):
				sobj = {}
				for a in attr:
					sobj[a] = t(data[focus],a,i)
				obj['ADMISSIONS'][t(data[focus],'HADM_ID',i)][focus].append(sobj)

		focuses = ['COMORBIDITY_SCORES']
		for focus in focuses:
			attr = allAttr[focus].split(",")
			for i in range(len(data[focus]['data'])):
				sobj = {}
				for a in attr:
					sobj[a] = t(data[focus],a,i)
				assert obj['ADMISSIONS'][t(data[focus],'HADM_ID',i)][focus]==None
				# print sobj
				if( obj['ADMISSIONS'][t(data[focus],'HADM_ID',i)][focus] == None):
					obj['ADMISSIONS'][t(data[focus],'HADM_ID',i)][focus] = sobj
	else:
		print "NO ADMISSIONS : file probably corrupted : "+ str(id) 
		if (len(data['ICD9']['data']) >=1):
			print "\tHAS ICD9 data"
		if (len(data['COMORBIDITY_SCORES']['data']) >=1):
			print "\tHAS ICD9 data"


	# -- 
	focuses = ['DRGEVENTS','MICROBIOLOGYEVENTS','PROCEDUREEVENTS']
	
	if( obj['ADMISSIONS'] == None ):
		if(EMBED_DRGEVENTS_MICROBIOLOGYEVENTS_PROCEDUREEVENTS):
			for focus in focuses:
				if len(data[focus]['data']) > 0:
					print "EMBED BUT HAS "+focus
		EMBED_DRGEVENTS_MICROBIOLOGYEVENTS_PROCEDUREEVENTS = False


	if(EMBED_DRGEVENTS_MICROBIOLOGYEVENTS_PROCEDUREEVENTS):
		for i in obj['ADMISSIONS']:
			for j in focuses:
				obj['ADMISSIONS'][i][j] = []

	for focus in focuses:
		attr = allAttr[focus].split(",")
		for i in range(len(data[focus]['data'])):
			sobj = {};
			for a in attr:
				sobj[a] = t(data[focus],a,i)
			if( EMBED_DRGEVENTS_MICROBIOLOGYEVENTS_PROCEDUREEVENTS):
				obj['ADMISSIONS'][sobj['HADM_ID']][focus].append(sobj)
			else:
				obj['ELSE'][focus].append(sobj)

	# -- ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL
	if( len(obj['ADMISSIONS']) == 0 ):
		if(EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL):
			for focus in focuses:
				if len(data[focus]['data']) > 0:
					print "EMBED BUT HAS "+focus
		EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL = False

	if( EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL):
		for k in obj['ADMISSIONS'].keys():
			obj['ADMISSIONS'][k]['ICUSTAY'] = {}
			obj['ADMISSIONS'][k]['DEMOGRAPHICEVENTS'] = []
			obj['ADMISSIONS'][k]['DEMOGRAPHIC_DETAIL'] = {}

			obj['ELSE']['ICUSTAYEVENTS'] = []
			obj['ELSE']['ICUSTAY_DETAIL'] = []
			obj['ELSE']['DEMOGRAPHICEVENTS'] = []
			obj['ELSE']['DEMOGRAPHIC_DETAIL'] = []
	else :
		obj['ELSE']['ICUSTAYEVENTS'] = []
		obj['ELSE']['ICUSTAY_DETAIL'] = []
		obj['ELSE']['DEMOGRAPHICEVENTS'] = []
		obj['ELSE']['DEMOGRAPHIC_DETAIL'] = []
	

	# # -- ICUSTAYDETAIL
	mapStayAdm = {};
	attr = allAttr['ICUSTAY_DETAIL'].split(",")
	attr = attr[0:-1]

	for i in range(len(data['ICUSTAY_DETAIL']['data'])):
		sobj = {}
		for a in attr:
			sobj[a] = t(data['ICUSTAY_DETAIL'],a,i)
		sid = t(data['ICUSTAY_DETAIL'],'ICUSTAY_ID',i)
		
		assert sid not in mapStayAdm
		mapStayAdm[sid] = t(data['ICUSTAY_DETAIL'],'HADM_ID',i)
		
		if (EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL):
			if(t(data['ICUSTAY_DETAIL'],'HADM_ID',i) == ''):
				obj['ELSE']['ICUSTAY_DETAIL'].append(sobj)
			else:
				obj['ADMISSIONS'][t(data['ICUSTAY_DETAIL'],'HADM_ID',i)]['ICUSTAY'][sid] = {}
				obj['ADMISSIONS'][t(data['ICUSTAY_DETAIL'],'HADM_ID',i)]['ICUSTAY'][sid]['DETAIL'] = sobj
		else:
			obj['ELSE']['ICUSTAY_DETAIL'].append(sobj)


	for i in range(len(data['ICUSTAYEVENTS']['data'])):
		attr = allAttr['ICUSTAYEVENTS'].split(",")
		sobj = {}
		for a in attr:
			sobj[a] = t(data['ICUSTAYEVENTS'],a,i)
		sid = t(data['ICUSTAYEVENTS'],'ICUSTAY_ID',i)
		if (EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL):
			for i in obj['ADMISSIONS']:
				if(sid in obj['ADMISSIONS'][i]['ICUSTAY']):
					obj['ADMISSIONS'][i]['ICUSTAY'][sid]['EVENTS'] = sobj
		else:
			obj['ELSE']['ICUSTAYEVENTS'].append(sobj)

	# -- DEMOGRAPHIC_DETAIL
	focuses = ['DEMOGRAPHIC_DETAIL']
	for focus in focuses:
		attr = allAttr[focus].split(",")
		for i in range(len(data[focus]['data'])):
			sobj = {}
			for a in attr:
				sobj[a] = t(data[focus],a,i)
			if (EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL):
				if(t(data[focus],'HADM_ID',i) == ''):
					obj['ELSE'][focus].append(sobj)
				else:
					assert obj['ADMISSIONS'][t(data[focus],'HADM_ID',i)][focus] == {}
					obj['ADMISSIONS'][t(data[focus],'HADM_ID',i)][focus]=sobj
			else:
				obj['ELSE'][focus].append(sobj)

	# -- DEMOGRAPHICEVENTS
	focuses = ['DEMOGRAPHICEVENTS']
	for focus in focuses:
		attr = allAttr[focus].split(",")
		for i in range(len(data[focus]['data'])):
			sobj = {}
			for a in attr:
				sobj[a] = t(data[focus],a,i)
			if (EMBED_ICUSTAY_DEMOGRAPHICEVENTS_DEMOGRAPHIC_DETAIL):
				if(t(data[focus],'HADM_ID',i) == ''):
					obj['ELSE'][focus].append(sobj)
				else:
					obj['ADMISSIONS'][t(data[focus],'HADM_ID',i)][focus].append(sobj)
			else:
				obj['ELSE'][focus].append(sobj)


	# #----
	focuses = ['IOEVENTS','LABEVENTS','MEDEVENTS','NOTEEVENTS','TOTALBALEVENTS', 'A_CHARTDURATIONS', 'A_IODURATIONS', 'A_MEDDURATIONS','ADDITIVES', 'CENSUSEVENTS','CHARTEVENTS','POE_ORDER','DELIVERIES']
	if( len(obj['ADMISSIONS']) == 0 ):
		if(EMBED_EVENTS):
			for focus in focuses:
				if len(data[focus]['data']) > 0:
					print "EMBED BUT HAS "+focus
		EMBED_EVENTS = False

	#gen
	if(EMBED_EVENTS):
		for hamdid in obj['ADMISSIONS'].keys():
			for sid in obj['ADMISSIONS'][hamdid]['ICUSTAY'].keys():
				toinst = {
					'IOEVENTS':[],
					'LABEVENTS':[],
					'MEDEVENTS':[],
					'NOTEEVENTS':[],
					'TOTALBALEVENTS':[],
					'A_CHARTDURATIONS':[],
					'A_IODURATIONS':[],
					'A_MEDDURATIONS':[],
					'ADDITIVES': [],
					'CENSUSEVENTS': [],
					'CHARTEVENTS': [],
					'DELIVERIES': [],
					'POE_ORDER': []
				}
				for k in toinst.keys():
					obj['ADMISSIONS'][hamdid]['ICUSTAY'][sid][k] = toinst[k]


	# -- ICUSTAY_DAYS // THIS IS DUPLICATED // DONT HAVE VALUE // DONT ACTIVATE
	# attr = allAttr['ICUSTAY_DAYS'].split(",")
	# for i in range(len(data['ICUSTAY_DAYS']['data'])):
	# 	sobj = {};
	# 	for a in attr:
	# 		sobj[a] = t(data['ICUSTAY_DAYS'],a,i)
	# 		sid = t(data['ICUSTAY_DAYS'],'ICUSTAY_ID',i)
	# 	if(EMBED_EVENTS):
	# 		obj['ADMISSIONS'][mapStayAdm[sid]]['ICUSTAY'][sid]['DAYS'].append(sobj) 
	# 	else:
	# 		obj['ICUSTAY_DAYS'].append(sobj) 

	for focus in focuses:
		attr = allAttr[focus].split(",")
		if(data[focus]['data']!=None):
			for i in range(len(data[focus]['data'])):
				sobj = {};
				for a in attr:
					sobj[a] = t(data[focus],a,i)
					sid = t(data[focus],'ICUSTAY_ID',i)
				
				if(EMBED_EVENTS):
					if(sid == '' or mapStayAdm[sid]==''):
						obj['ELSE'][focus].append(sobj)
					else:
						obj['ADMISSIONS'][mapStayAdm[sid]]['ICUSTAY'][sid][focus].append(sobj)
						
				else:
					obj['ELSE'][focus].append(sobj)

	obj = clearDataType(obj);
	obj = categorize(obj);
	
	# -- visualize
	fs = open ("result.txt", 'w');
	fs.write(json.dumps(obj))
	fs.close()
	return obj


def categorizeByID(obj,arr,field):
	total = {}
	for i in range(len(obj[arr] )):
		if obj[arr][i][field] not in total:
			total[obj[arr][i][field] ] = []
		total[ obj[arr][i][field] ].append(obj[arr][i])
	obj[arr+"_C"] = total

def categorize(obj):
	for i in obj['ADMISSIONS']:
		categorizeByID(obj['ADMISSIONS'][i], 'PROCEDUREEVENTS', 'ITEMID')
		categorizeByID(obj['ADMISSIONS'][i], 'DRGEVENTS', 'ITEMID')
		
		for j in obj['ADMISSIONS'][i]['ICUSTAY']:
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'IOEVENTS', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'TOTALBALEVENTS', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'CHARTEVENTS', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'LABEVENTS', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'ADDITIVES', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'A_CHARTDURATIONS', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'MEDEVENTS', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'DELIVERIES', 'IOITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'A_IODURATIONS', 'ITEMID')
			categorizeByID(obj['ADMISSIONS'][i]['ICUSTAY'][j], 'A_MEDDURATIONS', 'ITEMID')
		
		categorizeByID(obj['ELSE'], 'PROCEDUREEVENTS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'DRGEVENTS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'IOEVENTS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'TOTALBALEVENTS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'CHARTEVENTS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'LABEVENTS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'ADDITIVES', 'ITEMID')
		categorizeByID(obj['ELSE'], 'A_CHARTDURATIONS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'MEDEVENTS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'DELIVERIES', 'IOITEMID')
		categorizeByID(obj['ELSE'], 'A_IODURATIONS', 'ITEMID')
		categorizeByID(obj['ELSE'], 'A_MEDDURATIONS', 'ITEMID')
		pass;
	return obj

def clearDataType(obj):
	# -- D_PATIENTS
	obj['DIE_IN_HOSP'] 	= 1 if obj['HOSPITAL_EXPIRE_FLG']=='Y' else 0
	del obj['HOSPITAL_EXPIRE_FLG']
	changeType(obj, {'DOD':'date','DOB':'date','SUBJECT_ID':'int'})
	# print obj['DOB']
	
	# -- ADMISSIONS
	for i in obj['ADMISSIONS']:
		changeType(obj['ADMISSIONS'][i], {'ADMIT_DT':'date','DISCH_DT':'date','HADM_ID':'int'})	

		# -- ICD9
		for j in range(len(obj['ADMISSIONS'][i]['ICD9'])):
			changeType(obj['ADMISSIONS'][i]['ICD9'][j], {'SUBJECT_ID':'int','HADM_ID':'int','SEQUENCE':'int'})		

		# -- COMORBIDITY_SCORES
		changeType(obj['ADMISSIONS'][i]['COMORBIDITY_SCORES'], {'SUBJECT_ID':'int','HADM_ID':'int','CONGESTIVE_HEART_FAILURE':'int','CARDIAC_ARRHYTHMIAS':'int','VALVULAR_DISEASE':'int','PULMONARY_CIRCULATION':'int','PERIPHERAL_VASCULAR':'int','HYPERTENSION':'int','PARALYSIS':'int','OTHER_NEUROLOGICAL':'int','CHRONIC_PULMONARY':'int','DIABETES_UNCOMPLICATED':'int','DIABETES_COMPLICATED':'int','HYPOTHYROIDISM':'int','RENAL_FAILURE':'int','LIVER_DISEASE':'int','PEPTIC_ULCER':'int','AIDS':'int','LYMPHOMA':'int','METASTATIC_CANCER':'int','SOLID_TUMOR':'int','RHEUMATOID_ARTHRITIS':'int','COAGULOPATHY':'int','OBESITY':'int','WEIGHT_LOSS':'int','FLUID_ELECTROLYTE':'int','BLOOD_LOSS_ANEMIA':'int','DEFICIENCY_ANEMIAS':'int','ALCOHOL_ABUSE':'int','DRUG_ABUSE':'int','PSYCHOSES':'int','DEPRESSION':'int'})		

		# -- ICUSTAY_DETAIL
		for j in obj['ADMISSIONS'][i]['ICUSTAY']:
			changeType(obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL'], {'ICUSTAY_ID':'int','SUBJECT_ID':'int','DOD':'date','DOB':'date','SUBJECT_ICUSTAY_TOTAL_NUM':'int','SUBJECT_ICUSTAY_SEQ':'int','HADM_ID':'int','HOSPITAL_TOTAL_NUM':'int','HOSPITAL_SEQ':'int','HOSPITAL_ADMIT_DT':'date','HOSPITAL_DISCH_DT':'date','HOSPITAL_LOS':'int','ICUSTAY_TOTAL_NUM':'int', 'ICUSTAY_SEQ':'int','ICUSTAY_INTIME':'datetime','ICUSTAY_OUTTIME':'datetime','ICUSTAY_ADMIT_AGE':'float','HEIGHT':'float','WEIGHT_FIRST':'float','WEIGHT_MIN':'float','WEIGHT_MAX':'float','SAPSI_FIRST':'int','SAPSI_MIN':'int','SAPSI_MAX':'int','SOFA_FIRST':'int','SOFA_MIN':'int','SOFA_MAX':'int','ICUSTAY_LOS':'int'})		
			
			if obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['EXPIRE_FLG']=='Y' : 
				obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['DIE_IN_AF_HOSP'] = 1  
			else:
				obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['DIE_IN_AF_HOSP'] = 0

			del obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['EXPIRE_FLG']

			if obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['HOSPITAL_EXPIRE_FLG']=='Y' : 
				obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['DIE_IN_HOSP'] = 1  
			else:
				obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['DIE_IN_HOSP'] = 0

			del obj['ADMISSIONS'][i]['ICUSTAY'][j]['DETAIL']['HOSPITAL_EXPIRE_FLG']
	
		# -- ICUSTAYEVENTS
		for j in obj['ADMISSIONS'][i]['ICUSTAY']:
			changeType(obj['ADMISSIONS'][i]['ICUSTAY'][j]['EVENTS'], {'ICUSTAY_ID':'int','SUBJECT_ID':'int','INTIME':'datetime','OUTTIME':'datetime','LOS':'int','FIRST_CAREUNIT':'int','LAST_CAREUNIT':'int'})

		# -- DEMOGRAPHIC_DETAIL
		changeType(obj['ADMISSIONS'][i]['DEMOGRAPHIC_DETAIL'], {'SUBJECT_ID':'int','HADM_ID':'int','MARITAL_STATUS_ITEMID':'int','ETHNICITY_ITEMID':'int','OVERALL_PAYOR_GROUP_ITEMID':'int','RELIGION_ITEMID':'int','ADMISSION_TYPE_ITEMID':'int','ADMISSION_SOURCE_ITEMID':'int'})

		# -- DEMOGRAPHICEVENTS
		for j in obj['ADMISSIONS'][i]['DEMOGRAPHICEVENTS']:
			changeType(j, {'ITEMID':'int','SUBJECT_ID':'int','HADM_ID':'int'})
			changeType(j['ITEMID_J'], {'ITEMID':'int'})

		# -- MICROBIO
		for j in obj['ADMISSIONS'][i]['MICROBIOLOGYEVENTS']:
			changeType(j, {'SUBJECT_ID':'int','HADM_ID':'int','SPEC_ITEMID':'int','ORG_ITEMID':'int','ISOLATE_NUM':'int','AB_ITEMID':'int','CHARTTIME':'datetime'})
			changeType(j['SPEC_ITEMID_J'], {'ITEMID':'int'})
			changeType(j['ORG_ITEMID_J'], {'ITEMID':'int'})
			changeType(j['AB_ITEMID_J'], {'ITEMID':'int'})
	 
		# -- DRGEVENTS
		for j in obj['ADMISSIONS'][i]['DRGEVENTS']:
			changeType(j, {'SUBJECT_ID':'int','HADM_ID':'int','ITEMID':'int','COST_WEIGHT':'float'})
			changeType(j['ITEMID_J'], {'ITEMID':'int'})
	 
		# -- PROCEDUREEVENTS
		for j in obj['ADMISSIONS'][i]['PROCEDUREEVENTS']:
			changeType(j, {'SUBJECT_ID':'int','HADM_ID':'int','ITEMID':'int','SEQUENCE_NUM':'int','PROC_DT':'date'})
			changeType(j['ITEMID_J'], {'ITEMID':'int'})

	
		for j in obj['ADMISSIONS'][i]['ICUSTAY']:
			# 'IOEVENTS':[],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['IOEVENTS']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','ALTID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','VOLUME':'float','UNITSHUNG':'int','NEWBOTTLE':'int'})
				changeType(k['CGID_J'], {'CGID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})
				changeType(k['ALTID_J'], {'ITEMID':'int'})

			# 'LABEVENTS':[],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['LABEVENTS']:
				changeType(k, {'SUBJECT_ID':'int','HADM_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','VALUENUM':'float'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})

			# 'MEDEVENTS':[],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['MEDEVENTS']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','VOLUME':'float','DOSE':'float','SOLUTIONID':'int','SOLVOLUME':'float'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['CGID_J'], {'CGID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})			

			# 'NOTEEVENTS':[],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['NOTEEVENTS']:
				changeType(k, {'SUBJECT_ID':'int','HADM_ID':'int','ICUSTAY_ID':'int','ELEMID':'int','CHARTTIME':'datetime','REALTIME':'datetime','CGID':'int','CUID':'int'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['CGID_J'], {'CGID':'int'})

			# 'TOTALBALEVENTS':[],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['TOTALBALEVENTS']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','PERVOLUME':'float','CUMVOLUME':'float','RESET':'int'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['CGID_J'], {'CGID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})

			# 'A_CHARTDURATIONS'
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['A_CHARTDURATIONS']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','ELEMID':'int','STARTTIME':'datetime','STARTREALTIME':'datetime','ENDTIME':'datetime','CUID':'int','DURATION':'int'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})

			# 'A_IODURATIONS'
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['A_IODURATIONS']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','ELEMID':'int','STARTTIME':'datetime','STARTREALTIME':'datetime','ENDTIME':'datetime','CUID':'int','DURATION':'int'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})

			# 'A_MEDDURATIONS':[],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['A_MEDDURATIONS']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','ELEMID':'int','STARTTIME':'datetime','STARTREALTIME':'datetime','ENDTIME':'datetime','CUID':'int','DURATION':'int'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})
	
			# 'ADDITIVES': [],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['ADDITIVES']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','IOITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','CGID':'int','CUID':'int','AMOUNT':'float'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['CGID_J'], {'CGID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})
				changeType(k['IOITEMID_J'], {'ITEMID':'int'})
	
			# 'CENSUSEVENTS': [],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['CENSUSEVENTS']:
				changeType(k, {'CENSUS_ID':'int','SUBJECT_ID':'int','INTIME':'datetime','OUTTIME':'datetime','CAREUNIT':'int','DESTCAREUNIT':'int','LOS':'float','ICUSTAY_ID':'int'})
				changeType(k['CAREUNIT_J'], {'CUID':'int'})
				changeType(k['DESTCAREUNIT_J'], {'CUID':'int'})
	
			# 'CHARTEVENTS': [],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['CHARTEVENTS']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','VALUE1NUM':'float','VALUE2NUM':'float'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['CGID_J'], {'CGID':'int'})
				changeType(k['ITEMID_J'], {'ITEMID':'int'})
			
			# 'DELIVERIES': [],
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['DELIVERIES']:
				changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','IOITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','CGID':'int','CUID':'int','RATE':'float'})
				changeType(k['CUID_J'], {'CUID':'int'})
				changeType(k['CGID_J'], {'CGID':'int'})
				changeType(k['IOITEMID_J'], {'ITEMID':'int'})
			
			# 'POE_ORDER': []
			for k in obj['ADMISSIONS'][i]['ICUSTAY'][j]['POE_ORDER']:
				changeType(k, {'POE_ID':'int','SUBJECT_ID':'int','HADM_ID':'int','ICUSTAY_ID':'int','START_DT':'datetime','STOP_DT':'datetime','ENTER_DT':'datetime','DOSES_PER_24HRS':'float','DURATION':'float','EXPIRATION_VAL':'float','EXPIRATION_DT':'datetime'})
				for l in k['POE_MED']: 
					changeType(l, {'POE_ID':'int','DOSE_VAL_DISP':'float'})


	# -- IOEVENTS
	for k in obj['ELSE']['IOEVENTS']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','ALTID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','VOLUME':'float','UNITSHUNG':'int','NEWBOTTLE':'int'})
		changeType(k['CGID_J'], {'CGID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})
		changeType(k['ALTID_J'], {'ITEMID':'int'})

	# 'LABEVENTS':[],
	for k in obj['ELSE']['LABEVENTS']:
		changeType(k, {'SUBJECT_ID':'int','HADM_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','VALUENUM':'float'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})

	# 'MEDEVENTS':[],
	for k in obj['ELSE']['MEDEVENTS']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','VOLUME':'float','DOSE':'float','SOLUTIONID':'int','SOLVOLUME':'float'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['CGID_J'], {'CGID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})			

	# 'NOTEEVENTS':[],
	for k in obj['ELSE']['NOTEEVENTS']:
		changeType(k, {'SUBJECT_ID':'int','HADM_ID':'int','ICUSTAY_ID':'int','ELEMID':'int','CHARTTIME':'datetime','REALTIME':'datetime','CGID':'int','CUID':'int'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['CGID_J'], {'CGID':'int'})

	# 'TOTALBALEVENTS':[],
	for k in obj['ELSE']['TOTALBALEVENTS']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','PERVOLUME':'float','CUMVOLUME':'float','RESET':'int'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['CGID_J'], {'CGID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})

	# 'A_CHARTDURATIONS'
	for k in obj['ELSE']['A_CHARTDURATIONS']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','ELEMID':'int','STARTTIME':'datetime','STARTREALTIME':'datetime','ENDTIME':'datetime','CUID':'int','DURATION':'int'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})

	# 'A_IODURATIONS'
	for k in obj['ELSE']['A_IODURATIONS']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','ELEMID':'int','STARTTIME':'datetime','STARTREALTIME':'datetime','ENDTIME':'datetime','CUID':'int','DURATION':'int'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})

	# 'A_MEDDURATIONS':[],
	for k in obj['ELSE']['A_MEDDURATIONS']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','ELEMID':'int','STARTTIME':'datetime','STARTREALTIME':'datetime','ENDTIME':'datetime','CUID':'int','DURATION':'int'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})

	# 'ADDITIVES': [],
	for k in obj['ELSE']['ADDITIVES']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','IOITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','CGID':'int','CUID':'int','AMOUNT':'float'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['CGID_J'], {'CGID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})
		changeType(k['IOITEMID_J'], {'ITEMID':'int'})

	# 'CENSUSEVENTS': [],
	for k in obj['ELSE']['CENSUSEVENTS']:
		changeType(k, {'CENSUS_ID':'int','SUBJECT_ID':'int','INTIME':'datetime','OUTTIME':'datetime','CAREUNIT':'int','DESTCAREUNIT':'int','LOS':'float','ICUSTAY_ID':'int'})
		changeType(k['CAREUNIT_J'], {'CUID':'int'})
		changeType(k['DESTCAREUNIT_J'], {'CUID':'int'})

	# 'CHARTEVENTS': [],
	for k in obj['ELSE']['CHARTEVENTS']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','ITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','REALTIME':'datetime','CGID':'int','CUID':'int','VALUE1NUM':'float','VALUE2NUM':'float'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['CGID_J'], {'CGID':'int'})
		changeType(k['ITEMID_J'], {'ITEMID':'int'})
	
	# 'DELIVERIES': [],
	for k in obj['ELSE']['DELIVERIES']:
		changeType(k, {'SUBJECT_ID':'int','ICUSTAY_ID':'int','IOITEMID':'int','CHARTTIME':'datetime','ELEMID':'int','CGID':'int','CUID':'int','RATE':'float'})
		changeType(k['CUID_J'], {'CUID':'int'})
		changeType(k['CGID_J'], {'CGID':'int'})
		changeType(k['IOITEMID_J'], {'ITEMID':'int'})
	
	# 'POE_ORDER': []
	for k in obj['ELSE']['POE_ORDER']:
		changeType(k, {'POE_ID':'int','SUBJECT_ID':'int','HADM_ID':'int','ICUSTAY_ID':'int','START_DT':'datetime','STOP_DT':'datetime','ENTER_DT':'datetime','DOSES_PER_24HRS':'float','DURATION':'float','EXPIRATION_VAL':'float','EXPIRATION_DT':'datetime'})
		for l in k['POE_MED']: 
			changeType(l, {'POE_ID':'int','DOSE_VAL_DISP':'float'})

	# -- MICROBIOLOGYEVENTS
	for j in obj['ELSE']['MICROBIOLOGYEVENTS']:
		changeType(j, {'SUBJECT_ID':'int','HADM_ID':'int','SPEC_ITEMID':'int','ORG_ITEMID':'int','ISOLATE_NUM':'int','AB_ITEMID':'int','CHARTTIME':'datetime'})
		changeType(j['SPEC_ITEMID_J'], {'ITEMID':'int'})
		changeType(j['ORG_ITEMID_J'], {'ITEMID':'int'})
		changeType(j['AB_ITEMID_J'], {'ITEMID':'int'})
 
	# -- DRGEVENTS
	for j in obj['ELSE']['DRGEVENTS']:
		changeType(j, {'SUBJECT_ID':'int','HADM_ID':'int','ITEMID':'int','COST_WEIGHT':'float'})
		changeType(j['ITEMID_J'], {'ITEMID':'int'})
 
	# -- PROCEDUREEVENTS
	for j in obj['ELSE']['PROCEDUREEVENTS']:
		changeType(j, {'SUBJECT_ID':'int','HADM_ID':'int','ITEMID':'int','SEQUENCE_NUM':'int','PROC_DT':'datetime'})
		changeType(j['ITEMID_J'], {'ITEMID':'int'})

	# -- ICUSTAY
	for j in obj['ELSE']['ICUSTAY_DETAIL']:
		changeType(j, {'ICUSTAY_ID':'int','SUBJECT_ID':'int','DOD':'date','DOB':'date','SUBJECT_ICUSTAY_TOTAL_NUM':'int','SUBJECT_ICUSTAY_SEQ':'int','HADM_ID':'int','HOSPITAL_TOTAL_NUM':'int','HOSPITAL_SEQ':'int','HOSPITAL_ADMIT_DT':'date','HOSPITAL_DISCH_DT':'date','HOSPITAL_LOS':'int','ICUSTAY_TOTAL_NUM':'int', 'ICUSTAY_SEQ':'int','ICUSTAY_INTIME':'datetime','ICUSTAY_OUTTIME':'datetime','ICUSTAY_ADMIT_AGE':'float','HEIGHT':'float','WEIGHT_FIRST':'float','WEIGHT_MIN':'float','WEIGHT_MAX':'float','SAPSI_FIRST':'int','SAPSI_MIN':'int','SAPSI_MAX':'int','SOFA_FIRST':'int','SOFA_MIN':'int','SOFA_MAX':'int','ICUSTAY_LOS':'int'})		
		
		if j['EXPIRE_FLG']=='Y' : 
			j['DIE_IN_AF_HOSP'] = 1  
		else:
			j['DIE_IN_AF_HOSP'] = 0
		del j['EXPIRE_FLG']

		if j['HOSPITAL_EXPIRE_FLG']=='Y' : 
			j['DIE_IN_HOSP'] = 1  
		else:
			j['DIE_IN_HOSP'] = 0
		del j['HOSPITAL_EXPIRE_FLG']

	# -- ICUSTAYEVENTS
	for j in obj['ELSE']['ICUSTAYEVENTS']:
		changeType(j, {'ICUSTAY_ID':'int','SUBJECT_ID':'int','INTIME':'datetime','OUTTIME':'datetime','LOS':'int','FIRST_CAREUNIT':'int','LAST_CAREUNIT':'int'})

	# -- DEMOGRAPHIC_DETAIL
	for j in obj['ELSE']['DEMOGRAPHIC_DETAIL']:
		changeType(j, {'SUBJECT_ID':'int','HADM_ID':'int','MARITAL_STATUS_ITEMID':'int','ETHNICITY_ITEMID':'int','OVERALL_PAYOR_GROUP_ITEMID':'int','RELIGION_ITEMID':'int','ADMISSION_TYPE_ITEMID':'int','ADMISSION_SOURCE_ITEMID':'int'})

	# -- DEMOGRAPHICEVENTS
	for j in obj['ELSE']['DEMOGRAPHICEVENTS']:
		changeType(j, {'ITEMID':'int','SUBJECT_ID':'int','HADM_ID':'int'})
		changeType(j['ITEMID_J'], {'ITEMID':'int'})
	return obj

jdata = readDef();

if __name__ == "__main__":
	patientObject(571);
	# for i in range(1,1000):
	# 	print i
	# 	# if i not in xid:
	# 	d = patientObject(i)
	