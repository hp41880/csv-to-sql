"encoding: utf-8"
"""->This program takes txt files in folder as input, processes it in chunks, 
   ->each chunk.PoI_No,chunk.PoA_No is cleaned,chunk.Date_of_Act is parsed for
     proper and uniform formatting,
   ->each chunk is then inserted into sqlite database
   ->index is created over certain fields of the database
   
   *commented part of script will read first 5 lines of each csv file in the
    folder and write it into head.csv
   *path=path of folder where txt files are. resultant database and head.csv 
    will also be saved here. CSV files should be utf-8 encoded,15 columns(see 'columns' list)
"""
import glob,gc,numpy as np
import pandas as pd
import sqlite3
import  shutil,dateutil


path =r'D:\\c1a_of_31June18\\' # use your path
#path=r"D:\\"
dest_path = path+"utf8\\"
files = glob.glob(path+'*.txt')    #current directory needs to be correct
columns=[ "NUM", "Name","Local_add","Perm_add","PoI_No","PoI_type","PoA_No","PoA_type","Date_of_Act","POS_Name","POS_Add","Status","Conn_Type","Gender","filename"]
dtype={ "NUM":np.int64,"Name":str,"Local_add":str,"Perm_add":str,"PoI_No":str,"PoI_type":str,"PoA_No":str,"PoA_type":str,"Date_of_Act":str,"POS_Name":str,"POS_Add":str,"Status":str,"Conn_Type":str,"Gender":str,"filename":str}   
def run_query(query):
    conn=sqlite3.connect(path+'2018June31-formatted-DoA.sqlite')
    conn.text_factory = str
    c=conn.cursor()
    fetched_list=c.execute(query).fetchall()
    return fetched_list
    conn.commit()
    conn.close()

def search_N_replace(columnname,str_to_find,str_to_replace):
    conn=sqlite3.connect(path+'2018June31-formatted-DoA.sqlite')
    c=conn.cursor()
    query="UPDATE cdbs SET "+columnname+"='" +str_to_replace+"' WHERE "+columnname+"='"+str_to_find+"'"
    c.execute(query).fetchall()    
    conn.commit()
    conn.close()

#"This part will read first 5 lines of each csv file in the folder and write it into head.csv"
#for txtfile in files:
#    with open(txtfile,encoding="utf8") as myfile:
#        firstNlines=myfile.readlines()[0:5]
#        with open('head.csv','a') as fd:
#            for l in firstNlines:
#                fd.write(l)



conn = sqlite3.connect(path+"2018June31-formatted-DoA.sqlite")
c = conn.cursor()

c.execute('''drop table cdbs''')
c.execute('''CREATE TABLE cdbs(NUM integer NOT NULL, Name text,Local_add text,Perm_add text,Date_of_Act text,formatted_Date_of_Act datetime,PoI_type text,PoI_No text,PoA_type text,PoA_No text,POS_Name text,POS_Add text,Status text,Conn_Type text,Gender text,filename text NOT NULL)''')


total_records=0
i=0
for txtfile in files:
    i=i+1
    print(str(i)+"st file----"+str(txtfile))
    rownum=0
    for chunk in pd.read_csv(txtfile,sep=',',chunksize=5000,names=columns,skiprows=1,dtype={  "NUM":np.int64,"Name":str,"Local_add":str,"Perm_add":str,"PoI_No":str,"PoI_type":str,"PoA_No":str,"PoA_type":str,"Date_of_Act":str,"POS_Name":str,"POS_Add":str,"Status":str,"Conn_Type":str,"Gender":str,"filename":str}):
        
        
        chunk.PoA_No.replace("[^0-9a-zA-Z]+",'',regex=True, inplace=True)
        chunk.PoI_No.replace("[^0-9a-zA-Z]+",'',regex=True, inplace=True)    #accept only alphanumeric characters

        chunk.PoA_No.replace("\b(\w)\1+\b",'',regex=True, inplace=True)
        chunk.PoI_No.replace("\b(\w)\1+\b",'',regex=True, inplace=True)  #removing 1111,eeee,xxxx etc

        chunk.PoA_No.replace("^([A-Za-z0-9]{0,3})$",'',regex=True, inplace=True)   #removing PoI of length<5
        chunk.PoI_No.replace("^([A-Za-z0-9]{0,3})$",'',regex=True, inplace=True)   #removing PoI of length<5

        chunk['PoA_No'].apply(lambda x: 1 if(len(str(x)) < 4) else str(x))
        chunk['PoI_No'].apply(lambda x: 1 if(len(str(x)) < 4) else str(x))

        chunk.PoA_No=chunk.PoA_No.map(lambda x:str(x).lstrip('0'))
        chunk.PoI_No=chunk.PoI_No.map(lambda x:str(x).lstrip('0'))
        
        ####################
        """This part of program converts (by smartly guessing using python dateutil parser) 
        Date of Activation into correct format"""
        t=[]
        for x in chunk.Date_of_Act:
            try:
                u=dateutil.parser.parse(x)     
            except:
                u=x
            
            t.append(u)
        chunk['formatted_Date_of_Act']=t
        #################
        chunk.to_sql("cdbs", con=conn, if_exists="append", index=False)
        rownum=rownum+len(chunk)
        
    print(rownum)
    gc.collect()
    total_records=total_records+rownum
    dst=dest_path+txtfile[20:]
    src=txtfile
    shutil.move(src,dst)

print("total-----"+str(total_records)+"-------inserted in database")
    
conn.commit()
conn.close()

conn = sqlite3.connect(path+"2018June31.sqlite")
c = conn.cursor()

s7='CREATE INDEX index_on_NUM ON cdbs(NUM)'
run_query(s7)
print("s7 complete")

s1='CREATE INDEX index_on_Status ON cdbs(Status)'
run_query(s1)
print("s1 complete")

s2='CREATE INDEX index_on_PoI_No ON cdbs(PoI_No)'
run_query(s2)
print("s2 complete")

s3='CREATE INDEX index_on_PoA_No ON cdbs(PoA_No)'
run_query(s3)
print("s3 complete")

s4='CREATE INDEX index_on_Conn_Type ON cdbs(Conn_Type)'
run_query(s4)
print("s4 complete")

s6='CREATE INDEX index_on_filename ON cdbs(filename)'
run_query(s6)
print("s6 complete")



s8='analyze'
run_query(s8)
print("s8 complete")
conn.commit()
conn.close()



