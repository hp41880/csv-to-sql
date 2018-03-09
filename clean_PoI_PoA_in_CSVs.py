import glob,gc,csv,numpy as np
import pandas as pd
import sqlite3

path =r'J:\\c1a of 31stDec17\\csvs\\' # use your path
#path=r"D:\\"
files = glob.glob(path+'*.txt')    #current directory needs to be correct
columns=[ "NUM", "Name","Local_add","Perm_add","Date_of_Act","PoI_type","PoI_No","PoA_type","PoA_No","Status","Conn_Type","Gender","filename"]
dtype={ "NUM":np.int64, "Name":str,"Local_add":str,"Perm_add":str,"Date_of_Act":str,"PoI_type":str,"PoI_No":str,"PoA_type":str,"PoA_No":str,"Status":str,"Conn_Type":str,"Gender":str,"filename":str}   
def run_query(query):
    conn=sqlite3.connect(path+'analysis_after_ID_cleaned1.sqlite')
    c=conn.cursor()
    fetched_list=c.execute(query).fetchall()
    return fetched_list
    conn.commit()
    conn.close()

def search_N_replace(columnname,str_to_find,str_to_replace):
    conn=sqlite3.connect(path+'analysis_after_ID_cleaned1.sqlite')
    c=conn.cursor()
    query="UPDATE cdbs SET "+columnname+"='" +str_to_replace+"' WHERE "+columnname+"='"+str_to_find+"'"
    c.execute(query).fetchall()    
    conn.commit()
    conn.close()

conn = sqlite3.connect(path+"analysis_after_ID_cleaned1.sqlite")
c = conn.cursor()


c.execute('''CREATE TABLE cdbs(NUM integer NOT NULL, Name text,Local_add text,Perm_add text,Date_of_Act text,PoI_type text,PoI_No text,PoA_type text,PoA_No text,Status text,Conn_Type text,Gender text,filename text NOT NULL)''')


total_records=0
i=0
for txtfile in files:
    i=i+1
    print(str(i)+"st file----"+str(txtfile))
    rownum=0
    for chunk in pd.read_csv(txtfile,sep=',',chunksize=100000,names=columns,skiprows=1,dtype={ "NUM":np.int64, "Name":str,"Local_add":str,"Perm_add":str,"Date_of_Act":str,"PoI_type":str,"PoI_No":str,"PoA_type":str,"PoA_No":str,"Status":str,"Conn_Type":str,"Gender":str,"filename":str}):
        
        
        chunk.PoA_No.replace("[^0-9a-zA-Z]+",'',regex=True, inplace=True)
        chunk.PoI_No.replace("[^0-9a-zA-Z]+",'',regex=True, inplace=True)    #accept only alphanumeric characters

        chunk.PoA_No.replace("\b(\w)\1+\b",'',regex=True, inplace=True)
        chunk.PoI_No.replace("\b(\w)\1+\b",'',regex=True, inplace=True)  #removing 1111,eeee,xxxx etc

        chunk.PoI_No.replace("^([A-Za-z0-9]{0,3})$",'',regex=True, inplace=True)   #removing PoI of length<5
        chunk.PoI_No.replace("^([A-Za-z0-9]{0,3})$",'',regex=True, inplace=True)   #removing PoI of length<5

        chunk['PoA_No'].apply(lambda x: 1 if(len(str(x)) < 4) else str(x))
        chunk['PoI_No'].apply(lambda x: 1 if(len(str(x)) < 4) else str(x))

        chunk.PoA_No=chunk.PoA_No.map(lambda x:str(x).lstrip('0'))
        chunk.PoI_No=chunk.PoI_No.map(lambda x:str(x).lstrip('0'))

        chunk.to_sql("cdbs", con=conn, if_exists="append", index=False)
        rownum=rownum+len(chunk)
    print(rownum)
    gc.collect()
    total_records=total_records+rownum
print("total-----"+str(total_records)+"-------inserted in database")
    
conn.commit()

uniq_ID=c.execute("SELECT DISTINCT(PoI_No) FROM cdbs UNION SELECT DISTINCT(PoA_No) FROM cdbs").fetchall()
myfile=open(path+"uniq_ID.csv","w",encoding="utf-8",newline='')
with myfile:
    writer=csv.writer(myfile)
    writer.writerows(uniq_ID)

conn.close()




