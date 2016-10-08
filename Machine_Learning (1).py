# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 16:03:44 2016

@author: rezeh001c
"""

if ('sim' not in locals() or 'sim' not in globals()):
    
    import gensim
    from gensim import corpora, models, similarities
    from wordsegment import segment
    import pandas as pd 
    from nltk.corpus import stopwords
    from bs4 import BeautifulSoup 
    import re
    import pyodbc
    import pickle
    import redis
    import pymysql
    
    
    r = redis.StrictRedis(host='10.251.127.51', port = 57117,password='5c114ed8-4e28-4a9d-966b-e326964d9615', decode_responses = True)

    # Create a file handle....
     
    
    connection = pymysql.connect(host='10.124.58.36', port=3306, user='rene', password='C0mcast!', db='Descartes') 
    train3 = pd.read_sql('SELECT ID, Title, Description from idea', \
    connection, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    
    
    #Read Comcast words from a file    
    lines = [line.lower().rstrip('\n') for line in open('comcastwords.txt')]


    # Modify Segment.py to exclude Comcast words when splitting words up.
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
      
    def idea_to_words1( raw_idea ):
        letters_only = ' '.join(letter(k) for k in raw_idea.lower().split() )
        words = letters_only.lower().split()                             
        stops = set(stopwords.words("english"))                  
        meaningful_words = ["".join(seg(w)) for w in words if not w in stops] 
        return( " ".join( meaningful_words )) # mw was  used for classifier5.pkl 
     
    def idea_to_words( raw_idea ):
        letters_only = ' '.join(letter(k) for k in raw_idea.lower().split() )
        words = letters_only.lower().split()                             
        stops = set(stopwords.words("english"))                  
        meaningful_words = ["".join(seg(w)) for w in words if not w in stops] 
        return meaningful_words  # mw was  used for classifier5.pkl 

    num_ideas = train3["DESCRIPTION"].size  
    
#    cursor = connection.cursor()
#    for i in xrange( 0, num_ideas ):
#        # Call our function for each one, and add the result to the list of clean ideas
#        #if( (i+1)%100 == 0 ):
#            print " Retrieving Idea %d of %d\n" % ( i+1, num_ideas )     
#        k=  "\""+idea_to_words1( train3["Title"][i]+train3["Description"][i] )+ "\""
#        l= int(train3['ID'][i])
#        #varlist = [2,k]
#        query = "INSERT INTO cleanIdeas (ID,cleanIdea) VALUES ("+str(l)+","+k+")"
#        print(query)
#        #query = "INSERT INTO cleanIdeas SET ID= int(train3['ID'][i]), cleanIdea=clean_ideas[i]"
#        cursor.execute(query)
#        connection.commit() 
    
    
    num_ideas = train3["DESCRIPTION"].size
    
    train4 = pd.read_sql('SELECT cleanIdea from cleanIdeas', \
    connection, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    
    clean=[]
    for i in xrange( 0, 6064 ):
        if( (i+1)%100 == 0 ):
            print " Retrieving Idea %d of %d\n" % ( i+1, num_ideas )     
        clean.append(train4['cleanIdea'][i].split())
        
    connection.close() 
    
    # Now build and train the model....  
    
    fcount = 500 
    d = gensim.corpora.Dictionary(clean) 
    c = [d.doc2bow(t) for t in clean] 
    tfidf_model = gensim.models.TfidfModel(c, d, normalize=True) 
    tfidf_corpus = tfidf_model[c] 
    lsi_model = gensim.models.LsiModel(corpus=tfidf_corpus,id2word=d,num_topics=fcount) 
    lsi_corpus = lsi_model[tfidf_corpus] 
    
    # Now build the indexes for Silimarity
    sim = gensim.similarities.Similarity(None, lsi_corpus, fcount, shardsize=2) 
    cnt = len(sim)
    
    obj=sim
    pickled_object = pickle.dumps(obj)
    r.set('indx', pickled_object)
    
else: 
         
    input_file = open('arguments.txt', 'r')
    x = int(input_file.readline())
    y = float(input_file.readline()[:5])
    
    numbest = x
    Threshold = y
    
    sim = pickle.loads(r.get('indx'))
    
    # Set the number of best matches to be returned as output 
    sim.num_best = numbest +1
    nb = sim.num_best
    #Set display option to 3000
    pd.options.display.max_colwidth = 3000
    
    idea = raw_input("Kindly avoid the use of abbreviations, special characters and numbers. Please enter an idea: ")
    #os.system("pause")

    qtext =  idea_to_words(idea) 
    query = lsi_model[tfidf_model[d.doc2bow(qtext)]] 
    
    a2 = sim[query] 
    
    #Threshold = args.Threshold
    t = a2[0][1]
    if round(t,1) >= Threshold :
        print("Best match is "  '\n' + train3["Title"][a2[1][0]]+' '+ train3["Description"][a2[1][0]] + '\n' )
        for i in xrange(2,nb):    
            print("Next best match is "  '\n' +train3["Title"][a2[nb-(nb-i)][0]]+' '+ train3["Description"][a2[nb-(nb-i)][0]] + '\n')
    else:
        print("Best match is "  '\n' + train3["Title"][a2[0][0]]+' '+train3["Description"][a2[0][0]] + '\n' '\n')
        for i in xrange(1,nb-1):    
            print("Next best match is "  '\n' + train3["Title"][a2[nb-(nb-i)][0]]+' '+train3["Description"][a2[nb-(nb-i)][0]] + '\n')
