#!/usr/bin/python

import newspaper
import re
#import timeit

# http://newspaper.readthedocs.io/en/latest/user_guide/quickstart.html#extracting-articles
class TechcrunchAgent():
	# This function returns all filtered Article URLs of Techcrunch
	def collectArticleUrls(self, db, endsiteUrl):
		print('Starting crawling of Techcrunch site')
		
		tc = newspaper.build(endsiteUrl, memoize_articles=False)
		# memoize_articles is required so newspaper doesn't cache previous parsing

		basicPattern='(https://techcrunch.com/)(\d{4}/\d{1,2}/\d{1,2})(/.*)$'
		extraPattern='^((?!#|\?).)*$'

		matchedSize=0;
		totSize=0;
		articleList=[]
		for article in tc.articles:
			totSize+=1
			url=article.url
			matched=re.match(basicPattern, url)
			# 1st check if basic URL pattern match is success
			if matched is not None:
				# 2nd check if matched URL contains # or ?, if yes then ignore those URLs
				publishDate=matched.group(2)
				matched=re.match(extraPattern, url)
				if matched is not None:
					# If code reaches here, then 1st and 2nd check is success
					dbRow=db.find("ARTICLES", {"url":url})
					if dbRow is not None:
						# this means that this url already exist in DB
						# TODO: This login needs to move to In_MEMORY hashing or REDIS cache, instead of DB hit
						# https://stackoverflow.com/questions/16008670/python-how-to-hash-a-string-into-8-digits
						print('Skipping existing DB record.')
						continue
					matchedSize+=1
					#print(matched.group())
					articleList.append((publishDate, article))
		print('##################################################################')
		print('{} URLs found'.format(tc.size()))
		print('{} URLs selected.'.format(matchedSize))
		return articleList	

	# This function sorts the Article list based on Publish date
	def sortArticleUrl(self, articleList):
		# Below sorting article list based on publishDate
		sortedList=sorted(articleList, key=lambda articleRec: articleRec[0])
		return sortedList
	

	# This function initializes the Article object(A python Dictionary variable) using Newspaper module's Article class
	def initializeArticleObject(self, articleObj):
		articleDictionary={}	# this dictionary is an Article's key-value pair. This will be inserted in DB
		
		articleRecord=articleObj[1]

		articleDictionary["url"]=articleRecord.url

		articleRecord.download()
		articleRecord.parse()
		authors=articleRecord.authors	# this is an array
		if not authors:
			print('Author not found by Newspaper')# list is empty or None
		else:
			articleDictionary["author"]=authors[0]
		articleDictionary["title"]=articleRecord.title
		articleDictionary["postBody"]=articleRecord.text
		articleDictionary["topImageUrl"]=articleRecord.top_image
		# Below 'articleRecord.images' is Python class type SET. Converted to class type LIST
		# else postgresql column IMAGE_URLS being type Array of String won't map correctly
		articleDictionary["imageUrls"]=list(articleRecord.images) 	
		
		articleRecord.nlp()
		articleDictionary["tags"]=articleRecord.keywords	# this is an array (Python class type=list)
		articleDictionary["postSummary"]=articleRecord.summary
		
		articleDictionary["like_count"]=0	# hard-coding this to Zero
		articleDictionary["blog_id"]=1
		articleDictionary["publishDate"]=articleObj[0]
		

		return articleDictionary

	# This function inserts 1 Article record in the ARTICLES table
	def insertRecord(self, db, articleObject):
		article=list(articleObject.values())
		# TODO: Enhance this method to accept input as keynames for each column. CUrrently we are
		# relying on placement of articleObject.values() index. This is vulnerable to errors
		db.upsert("INSERT INTO ARTICLES \
			(URL, AUTHOR, TITLE, POST_BODY, TOP_IMAGE_URL,  \
			IMAGE_URLS, TAGS, \
			POST_SUMMARY, LIKE_COUNT, BLOG_ID, PUBLISH_DATE) \
			VALUES \
			(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING ARTICLES_ID", article)