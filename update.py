# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 13:19:04 2016

@author: rezeh001c
"""

''' ADDING NEW docS TO THE INDEX '''
from bs4 import BeautifulSoup 
import re
from wordsegment import segment
import redis
import pymysql
import pyodbc
import pandas as pd
from nltk.corpus import stopwords   
import gensim
import pickle
    
r = redis.StrictRedis(host='xxxx', port = 55517,password='5c114ed8-4e28-4a9d-966b-e326964d9615', decode_responses = True)

# '''Create a file handle....'''
 

#connection = pymysql.connect(host='xxxxxx', port=3016, user='rezeh', password='regpassw!', db='server') 
#train3 = pd.read_sql('SELECT ID, Title, Description from doc', \
#connection, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
#
#connection = pyodbc.connect(driver= '{SQL Server};',
#                            server= 'server1d',
#                        database= 'Portal_UAT',
#                            uid='docsPortal', pwd= 'regpassw!')
#train4 = pd.read_sql('SELECT ID, TITLE, DESCRIPTION from suggestion', \
#connection, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)

connection = pyodbc.connect(driver= '{SQL Server};',
                            server= 'server',
                        database= 'Portal',
                            uid='docsPortal', pwd= 'regpassw!')

trainx = pd.read_sql('SELECT ID, TITLE, DESCRIPTION from portal WHERE portal.Id not in (select id from cleandocs)',\
connection, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)


def letter(w):
    if w.lower() not in lines:
        i=re.sub("[^a-zA-Z]", " ", w)
    else:
        i=w
    return i

def seg(w):
    if w.lower() in lines:
        r = w.lower()
    else:
        r= segment(w)
    return r
        
def doc_to_words1( raw_doc ):
    doc_text = BeautifulSoup(raw_doc, "lxml").get_text() 
    letters_only = re.sub("[^a-zA-Z(x1)']", " ", doc_text) 
    words = letters_only.lower().split()                             
    stops = set(stopwords.words("english"))                  
    meaningful_words = [" ".join(seg(w)) for w in words if not w in stops] 
    return( " ".join( meaningful_words )) # mw was  used for classifier5.pkl 

def doc_to_words( raw_doc ):
    doc_text = BeautifulSoup(raw_doc, "lxml").get_text() 
    letters_only = re.sub("[^a-zA-Z(x1)']", " ", doc_text) 
    words = letters_only.lower().split()                             
    stops = set(stopwords.words("english"))                  
    meaningful_words = [" ".join(seg(w)) for w in words if not w in stops] 
    return meaningful_words  

#num_docs=train4["DESCRIPTION"].size
#cursor = connection.cursor()
#for i in xrange( 0, len(train4) ):
#    k=  "\'"+doc_to_words1( train4["TITLE"][i]+train4["DESCRIPTION"][i] )+ "\'"
#    l= int(train4['ID'][i])
#    query = "INSERT INTO cleandocs (ID,cleandoc) VALUES ("+str(l)+","+k+")"
#    if( (i+1)%100 == 0 ):
#            print " Retrieving doc %d of %d\n" % ( i+1, num_docs ) 
#    #query = "INSERT INTO cleandocs SET ID= int(train3['ID'][i]), cleandoc=clean_docs[i]"
#    cursor.execute(query)
#    connection.commit() 


if not (len(trainx) == 0):
    new_docs=trainx["DESCRIPTION"].size
    cursor = connection.cursor()
    for i in xrange( 0, new_docs ):
        # Call our function for each one, and add the result to the list of clean docs
        k=  "\'"+doc_to_words1( trainx["TITLE"][i]+trainx["DESCRIPTION"][i] )+ "\'"
        l= int(trainx['ID'][i])
        query = "INSERT INTO cleandocs (ID,cleandoc) VALUES ("+str(l)+","+k+")"
        print(query)
        #query = "INSERT INTO cleandocs SET ID= int(train3['ID'][i]), cleandoc=clean_docs[i]"
        cursor.execute(query)
        connection.commit() 
       
        
    
    clean_test=[]
    s1=trainx["TITLE"][0]
    s2=trainx["DESCRIPTION"][0]
    for i in xrange( 0, len(trainx) ):
        clean_test.append(doc_to_words(s1+' '+s2))
    
    fcount=500
    d_new = gensim.corpora.Dictionary(clean_test)
    c_new = [d_new.doc2bow(p) for p in clean_test]
    tfidf_model_new = gensim.models.TfidfModel(c_new, d_new, normalize=True) 
    tfidf_corpus_new = tfidf_model_new[c_new] 
    lsi_model_new = gensim.models.LsiModel(corpus=tfidf_corpus_new,id2word=d_new,num_topics=fcount) 
    lsi_corpus_new = lsi_model_new[tfidf_corpus_new] 
    
    print(str(len(clean_test)) + " new record(s) added to the index")
    
    #Pull up existing index file
    sim2 = pickle.loads(r.get('indx'))

    # Add the new doc to the index file
    sim2.add_documents(lsi_corpus_new)
    
    #Store the new index file back to Redis
    obj=sim2
    pickled_object = pickle.dumps(obj)
    r.set('indx', pickled_object)
    print('Index file successfully updated')
else:
    print("No new record to add")    
connection.close() 
