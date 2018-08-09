#!/usr/bin/python

from TechcrunchAgent import TechcrunchAgent
from DB import DB
import timeit
from datetime import datetime

class CrawlerJob():
    def do(self):
        #https://docs.microsoft.com/en-us/visualstudio/python/vs-tutorial-01-02
        print('Inside do() method')

        startTime= datetime.now()
        db=DB()
        # TODO: Below DB connection string needs to be picked up from Heroku Env. variable
        db.create_global_connection(1, 1, 'postgres://wyewnhia:qktFlQcYreTusJDE1oNWTipmWvxO1qAs@baasu.db.elephantsql.com:5432/wyewnhia')
        timeElapsed=datetime.now()-startTime 
        print('>>> DB_ConnectionPool \t\tTime elapsed (hh:mm:ss.ms) {}'.format(timeElapsed))

        blogs=db.findAll("blogs", {"is_active":True})
        #blogs=db.findAll("articles", {"author":"Jon Russell", "publish_date":"2017-12-13"})
        if blogs is not None:
            columnList=blogs.pop(0)
            urlIndex=columnList.index("url")
            nameIndex=columnList.index("name")
            for blog in blogs:
                if blog[nameIndex].strip()=="TECH_CRUNCH":
                    agent=TechcrunchAgent()
                    endsiteUrl=blog[urlIndex]
                    self.crawl(db, agent, endsiteUrl)


    def crawl(self, db, agent, endsiteUrl):
		# TODO: Implement logging framework

        startTime= datetime.now()
        articleList=agent.collectArticleUrls(db, endsiteUrl)   # newspaper fetches all URLs inside this webpage
        timeElapsed=datetime.now()-startTime 
        print('>>> Collect_Article \t\tTime elapsed (hh:mm:ss.ms) {}\n'.format(timeElapsed))
        sortedList=agent.sortArticleUrl(articleList)

        counter=0
        for article in sortedList:
            startTime= datetime.now()
            articleObject=agent.initializeArticleObject(article)    # CPU intensive task
            timeElapsed=datetime.now()-startTime
            print('>>> Initialize_Article_NLP \tTime elapsed (hh:mm:ss.ms) {}'.format(timeElapsed))

            #print(articleObject , "\n******\n")

            startTime= datetime.now()
            agent.insertRecord(db, articleObject)
            timeElapsed=datetime.now()-startTime 
            print('>>> DB_Insert_1_Record \t\tTime elapsed (hh:mm:ss.ms) {}'.format(timeElapsed))
            print("\n")

			#counter+=1
			#if counter>=1:
			#	break

job=CrawlerJob()
startTime= datetime.now()
job.do()
timeElapsed=datetime.now()-startTime 
print('>>> Total_Crawl \t\tTime elapsed (hh:mm:ss.ms) {}'.format(timeElapsed))