import pandas as pd
from itertools import chain
import numpy as np
import json

with open('Entities.json') as f:
    data = json.load(f)

df_entity_orginal = pd.DataFrame({'Entity' : []})
# print(len(data))
for i in range(len(data)):
	df_entity_orginal = df_entity_orginal.append({'Entity': data[i]["name"]}, ignore_index=True)
	# print(data[i]["name"])
print(df_entity_orginal)

data_topic_map = pd.read_csv("Event_Document_Map_k2.csv", encoding = "ISO-8859-1")
data_entity = pd.read_csv("result_NET.csv", encoding = "ISO-8859-1")
for row in data_topic_map.iterrows():
	# print(row)
	df_empty = pd.DataFrame({'Document' : []})
	str1 = row[1]['Document'][1:-1]
	if "," not in str1: 
		continue
	list1 = [x.strip() for x in str1.split(',')]
	# print(list1)
	for i in range(len(list1)):
		df_empty = df_empty.append({'Document': list1[i]}, ignore_index=True)
	df_empty['Document'] = df_empty['Document'].astype(int)
	# print(df_empty)
	# print(data_entity.dtypes)
	df_empty = df_empty.join(data_entity.set_index('Document'), on='Document')
	df_empty = df_empty.loc[df_empty['Entities'] != '[]']
	# df_empty = df_empty.dropna(subset=['Entities'])
	# print(df_empty)
	df_empty2 = pd.DataFrame({'Entity' : [], 'Event'+str(row[0]): []})
	for row1 in df_empty.iterrows():
		str2 = row1[1]['Entities'][1:-1]
		if "," not in str2: 
			continue
		list1 = [x.strip() for x in str2.split(',')]
		for i in range(len(list1)):
			df_empty2 = df_empty2.append({'Entity': list1[i], 'Event'+str(row[0]): row[0]}, ignore_index=True)
		# list2.append(list1)
	df_empty2['Event'+str(row[0])] = df_empty2['Event'+str(row[0])].astype(int)
	df_empty2 = df_empty2.drop_duplicates()
	print(df_empty2)
	df_entity_orginal = df_entity_orginal.join(df_empty2.set_index('Entity'), on='Entity')
	df_entity_orginal['Event'+str(row[0])] = df_entity_orginal['Event'+str(row[0])].fillna(100)
	# df_entity_orginal['Event'+str(row[0])] = df_entity_orginal['Event'+str(row[0])].astype(int)
# source_col_loc = df_entity_orginal.columns.get_loc('Entity') # column position starts from 0
# df_entity_orginal['Events'] = df_entity_orginal.iloc[:,source_col_loc+1:source_col_loc+45].apply(lambda x: ",".join(x.astype(str)), axis=1).dropna()

print(df_entity_orginal)
df_empty_final = pd.DataFrame({'Entity' : [], 'Event': []})
# df_entity_orginal.to_csv('Entity_Event.csv', encoding='utf-8')
for row in df_entity_orginal.iterrows():
	list1 = []
	for i in range(28):
		if(row[1]['Event'+str(i)]!= 100):
			list1.append(int(row[1]['Event'+str(i)]))
	print(list1)
	df_empty_final = df_empty_final.append({'Entity': row[1]['Entity'], 'Event': list1}, ignore_index=True)
print(df_empty_final)
df_empty_final.to_csv('Entity_Event_k2.csv', encoding='utf-8')
	# break
