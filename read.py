#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import time
start_time = time.time()
print(" ----- %s time----"%(start_time))
pd.set_option('display.max_rows', None)

anot_loc=4
time_loc=1
anot_stat_loc=5
start_time_loc=9
end_time_loc=10

df=pd.read_csv("/home/abhi/Desktop/asg/aruba/data",delim_whitespace = True,nrows=500,header=None,names=["date","time","sensor","status","anot","anot_stat","cur","event_no","count","start_time","end_time"])


print(df.shape[0])

#df["date"]=pd.to_datetime(df["date"]) 
#df["time"]=pd.to_datetime(df["time"])
#df["start_time"]=df["start_time"].astype(str)
#df["end_time"]=df["end_time"].astype(str)

df.loc[:,"date"]=pd.to_datetime(df["date"])
df.loc[:,"time"]=pd.to_datetime(df["time"])



for i in range(0,df.shape[0]):
    if(df.iloc[i,anot_loc+1]== "begin"):
        df.iat[i,start_time_loc]=df.iloc[i,time_loc]
df=df.ffill()
#print(df1)



            
leng1=df.shape[0] 
prv_act=df.iloc[0,anot_loc]
prv_stat=df.iloc[0,anot_loc+1]
     
if(df.iloc[0,anot_loc-1]== "ON" or df.iloc[0,anot_loc-1]=="OPEN" ):
    df.iat[0,anot_loc+4]=1
elif(df.iloc[0,anot_loc-1]== "OFF"):
    df.iat[0,anot_loc+4]=0

#print("len")

for i in range(1,leng1):
    #print(i)
    if(df.iloc[i,anot_loc]==prv_act and df.iloc[i,anot_loc+1]== prv_stat and prv_stat=="end"):
        df.iat[i,anot_loc+2]=1
        
    if(df.iloc[i,anot_loc-1]== "ON"):
        df.iat[i,anot_loc+4]=1
    elif(df.iloc[i,anot_loc-1]== "OPEN"):
        df.iat[i,anot_loc+4]=1
    
        
    prv_act=df.iloc[i,anot_loc]
    prv_stat=df.iloc[i,anot_loc+1]
   # df.iat[i,anot_loc]="del"

#df
print("---time taken to read and process anot status %s seconds ---" % (time.time() - start_time))


# In[2]:



lst= []

for i in range(1,leng1):
    if(df.iloc[i,anot_loc+2]==1):
        lst.append(i)
        
df2=df.drop(lst,axis=0)
df2=df2.reset_index(drop=True)

print("---time taken to drop end lines %s seconds ---" % (time.time() - start_time))


# In[3]:




leng2= df2.shape[0]

evnt=1

for i in range(0,leng2):
    
    
    df2.iat[i,anot_loc+3]=evnt
    if(df2.iloc[i,anot_loc+1]=="end"):
        evnt+=1
        #df2.iat[i,end_time_loc]=df2.iloc[i,time_loc]
      
for i in range(0,leng2):
     if(df2.iloc[i,anot_loc+1]=="end"):
            df2.iat[i,end_time_loc]=df2.iloc[i,time_loc]
        
  


#df2[["start_time"]]=df2[["start_time"]].bfill  
#df20=df2[["end_time","count" ]]
df20=df2[["end_time"]].bfill()
df2.loc[:,"end_time"]=df20

print("---tiime taken to attach event no an end time %s seconds ---" % (time.time() - start_time))


# In[4]:




df3=df2.pivot(columns="sensor",values= "count")
df4=df2.groupby(["event_no"])

df5=df4
df_f=pd.DataFrame(columns=df3.columns)
df_f["event_no"]=np.NAN
#print(len(df4))
for i ,j in df4:
    j=j.pivot(columns="sensor",values= "count")
    df_f.at[i]=j.sum()
    df_f.at[i,"event_no"]=i
    
    
print("---time taken to count sensor numbers %s seconds ---" % (time.time() - start_time))


# In[9]:


df8=df2.groupby("event_no")

print("---time taken by group by %s seconds ---" % (time.time() - start_time))


# In[6]:


lst1=[]
for i in range(0,df2.shape[0]):
    if(df2.iloc[i,anot_stat_loc]=="end"):
        pass
    else:
        lst1.append(i)
        

        
df9=df2.drop(lst1,axis=0)
df9=df9.reset_index(drop=True)

print("---time taken to drop last rows %s seconds ---" % (time.time() - start_time))


# In[7]:


df10=df9[["anot","date","start_time","end_time","event_no"]]


print("---time taken to make new frame %s seconds ---" % (time.time() - start_time))


# In[8]:


df11=pd.merge(df10,df_f, on="event_no")

print("---time taken to marge the final table %s seconds ---" % (time.time() - start_time))
print(df11.head())
df11.to_csv("file.csv")

# In[ ]:
df11.loc[:, 'start_time'] = pd.to_datetime(df11.loc[:, 'start_time'])

df11.loc[:, 'end_time'] = pd.to_datetime(df11.loc[:, 'end_time'])

df11[:,'duration'] = df11['end_time'] - df11['start_time']

dayOfWeek = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
df11['weekday'] = df11['date'].dt.dayofweek.map(dayOfWeek)


def season_of_date(date):
    year = str(date.year)
    seasons = {'spring': pd.date_range(start='21/03/' + year, end='20/06/' + year),
               'summer': pd.date_range(start='21/06/' + year, end='22/09/' + year),
               'autumn': pd.date_range(start='23/09/' + year, end='20/12/' + year)}
    if date in seasons['spring']:
        return 'spring'
    if date in seasons['summer']:
        return 'summer'
    if date in seasons['autumn']:
        return 'autumn'
    else:
        return 'winter'


# Assuming df has a date column of type `datetime`
df11['season'] = df11.date.map(season_of_date)

df11.drop(['T001', 'T002', 'T003', 'T004', 'T005', 'P001'], axis=1, inplace=True)

print("---time taken to complete this file is =   %s seconds ---" % (time.time() - start_tim))
print(df11.head())
df11.to_csv(out_fl_nam)
df11['date']=pd.to_datetime(df11['date']).dt.date
df11['start_time']=pd.to_datetime(df11['start_time']).dt.time
df11['end_time']=pd.to_datetime(df11['end_time']).dt.time

df11.drop(['ENTERHOME'],axis=1,inplace=True)
df11.to_csv("aruba2.csv")


