# -*- coding: utf-8 -*-
import pandas as pd
from itertools import chain
import numpy as np
from IPython import get_ipython
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
import sys
sys.path.insert(1, r'./../functions')  # add to pythonpath
from detect_cusum import detect_cusum



data_topic_map = pd.read_csv("doc_topic_map.csv", encoding = "ISO-8859-1")
# print(data)

# for i in range(0,25):
# 	data1 = data.loc[data['Topic'] == i]
# 	data1 = data1['Document'].tolist()
	#print(data1)

data_NET = pd.read_csv("result_NET.csv", encoding = "ISO-8859-1")
# print(data_NET)

data_merge = pd.merge(data_topic_map, data_NET, on='Document')
# print(data_merge)
data_merge['Date'] = data_merge[['Date', 'Time']].apply(lambda x: ' '.join(x), axis=1)
data_merge['Date'] = data_merge[['Date', 'AM/PM']].apply(lambda x: ' '.join(x), axis=1)
print(data_merge['Date'])
data_merge["Date"] = pd.to_datetime(data_merge.Date)


data_merge['year'] = data_merge['Date'].dt.year
data_merge['month'] = data_merge['Date'].dt.month
data_merge['Day'] = data_merge['Date'].dt.day
data_merge['Hour'] = data_merge['Date'].dt.hour

# print(data_merge['month'])
column_list = ['Id','Topic','Start','End','Document']
event_df =  pd.DataFrame( columns=column_list)
# print(event_df.columns)

topic_doc_column_list = ['Topic', 'Document', 'Event', 'TimeLine', 'SE']
topic_doc_df =  pd.DataFrame( columns=topic_doc_column_list)

topic_time_Doc_list = ['Topic', 'Time', 'Document']
topic_time_Doc_df =  pd.DataFrame( columns=topic_time_Doc_list)

count = 0
df_empty = pd.DataFrame({'Document' : []})
count_doc = 0 
for m in range(0,25):
	data_merge1 = data_merge.loc[data_merge['Topic'] == m]
	Doc_list_cou = []
	count_time = 0
	for i in range (data_merge.Date.min().day,data_merge.Date.max().day+1):
		list1 = []
		for j in range(1,5):
			data_final = data_merge1.loc[(data_merge1['Day'] == i) & (data_merge1['Hour'] < 6*j) & (data_merge1['Hour'] > 6*(j-1))]
			Doc_list_cou.append(len(data_final.index))
			list1 = list(data_final['Document'])
			topic_time_Doc_df.loc[count_doc] = [m, count_time, list1]
			count_doc = count_doc + 1
			count_time = count_time + 1
	print(Doc_list_cou)
	
	# calculate mean
	mean = sum(Doc_list_cou) / len(Doc_list_cou)

	
	var_res = np.std(Doc_list_cou)
	#print(m)
	#print(var_res)
	ta, tai, taf, amp = detect_cusum(Doc_list_cou, var_res*3, mean/1.25, True, False)
	print(tai,taf)
	# print(data_merge1)
	event_list = []
	for s in range(len(tai)):
		# event_df['id'] = count 
		# count = count + 1
		# event_df['Start'] = tai[s]
		# event_df['End'] = taf[s]
		# year_start = int(tai[s]/12) + 9
		# month_start = tai[s] % 12
		# year_end = int(taf[s]/12) +2015
		# month_end = taf[s] % 12
		day_start = int(tai[s]/4) + 9
		day_end = int(taf[s]/4) + 9
		hour_start = tai[s] % 4
		hour_end = taf[s] % 4
		list1 = []
		SE = []
		for x in range (day_start, day_end + 1):
			min_1 = 1
			max_1 = 5
			if x == day_start:
				min_1 = hour_start + 1
			if x == day_end:
				max_1 = hour_end + 2
			for y in range(min_1, max_1):
				data_final = data_merge1.loc[(data_merge1['Day'] == x) & (data_merge1['Hour'] < 6*y) & (data_merge1['Hour'] > 6*(y-1))]
				# print(list(data_final['Document']))
				new = data_final[['Document'].copy()]
				frames = [df_empty, new]
				df_empty = pd.concat(frames)
				list1 = list1 + list(data_final['Document'])
		#print(list1)	
		# event_df['Document'] = list1	
		event_df.loc[count] = [count,m,tai[s],taf[s],list1]
		event_list.append(count)
		# SE = SE + tai[s]
		# SE = SE + taf[s]
		data1 = data_topic_map.loc[data_topic_map['Topic'] == m]
		doc_list = data1['Document'].tolist()
		topic_doc_df.loc[count] = [m, doc_list, [count], Doc_list_cou, [tai[s], taf[s]]]
		count = count +1 
	# print(doc_list)
	# topic_doc_df.loc[m] = [m, doc_list, event_list, Doc_list_cou]
	# data1['Event'] = event_list
print(event_df['Document'])
# print(topic_doc_df['Document'])
event_df.to_csv('Event_Document_Map_k3.csv', encoding='utf-8')
topic_doc_df.to_csv('Topic_Document_Event_Map_k3.csv', encoding='utf-8')
df_empty['Document'] = df_empty['Document'].astype(int)

df_empty = pd.merge(df_empty, data_NET, on='Document')
print(df_empty)

df_empty = df_empty.loc[(df_empty['Entities'] != '[]')]

print(df_empty)
# print(df_empty.shape)
df_empty.to_csv('Document_Entity_k3.csv', encoding='utf-8')
topic_time_Doc_df.to_csv('Topic_Time_Doc_k3.csv', encoding='utf-8')