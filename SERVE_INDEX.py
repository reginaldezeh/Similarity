# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 13:19:04 2016

@author: rezeh001c
"""

''' ADDING NEW IDEAS TO THE INDEX '''

connection = pyodbc.connect(driver= '{SQL Server};',
                            server= 'ipdb-dt-1d',
                        database= 'SuggestionPortal_UAT',
                            uid='IdeasPortal_UAT', pwd= 'c0mcast!')

train2 = pd.read_sql('SELECT ID, TITLE, DESCRIPTION from  ( SELECT ID, TITLE, DESCRIPTION, ROW_NUMBER() OVER (ORDER BY ID) as row FROM suggestion ) a WHERE row >' +str(cnt) ,\
connection, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)

connection.close()

#idea_to_words1( train2["TITLE"][i].get_values()[0]+train2["DESCRIPTION"][i].get_values()[0] )


clean_test=[]
s1=train2["TITLE"][0]
s2=train2["DESCRIPTION"][0]
for i in xrange( 0, len(train2) ):
    clean_test.append(idea_to_words1(s1+' '+s2))

d_new = gensim.corpora.Dictionary(clean_test)
c_new = [d_new.doc2bow(p) for p in clean_test]
tfidf_model_new = gensim.models.TfidfModel(c_new, d_new, normalize=True) 
tfidf_corpus_new = tfidf_model[c_new] 
lsi_model_new = gensim.models.LsiModel(corpus=tfidf_corpus_new,id2word=d_new,num_topics=fcount) 
lsi_corpus_new = lsi_model[tfidf_corpus_new] 
sim.add_documents(lsi_corpus_new)

cnt= len(sim)
print(str(len(clean_test)) + " new record(s) added to the index")


