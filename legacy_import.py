from settings import *
import soundcloud
import six
import requests
import simplejson
import shlex
from py2neo import authenticate, Graph

client = soundcloud.Client(client_id=SOUNDCLOUD_CLIENT_ID)
authenticate("localhost:7474", NEO4J_USERNAME, NEO4J_PASSWORD)  
graph = Graph()

page_size = '100'
search_query = 'ambient'	#or whatever you want your database to be about

# uniqueness constraints for soundcloud data
graph.cypher.execute('CREATE CONSTRAINT ON (track:Track) ASSERT track.id IS UNIQUE')
graph.cypher.execute('CREATE CONSTRAINT ON (tag:Tag) ASSERT tag.name IS UNIQUE')

# uniqueness constraints for conceptnet data
graph.cypher.execute('CREATE CONSTRAINT ON (concept:Concept) ASSERT concept.id IS UNIQUE')

# Concept Import Query
addConceptNetData = """
WITH {json} AS document
UNWIND document.edges AS edges
WITH 
edges.start as startID,
SPLIT(edges.start,"/")[2] AS startLanguage,
SPLIT(edges.start,"/")[3] AS startConcept,
SPLIT(edges.start,"/")[4] AS startPartOfSpeech,
SPLIT(edges.start,"/")[5] AS startSense,
SPLIT(edges.rel,"/")[2] AS relType,
edges.surfaceText AS surfaceText,
edges.weight AS weight,
edges.end as endID,
SPLIT(edges.end,"/")[2] AS endLanguage,
SPLIT(edges.end,"/")[3] AS endConcept,
SPLIT(edges.end,"/")[4] AS endPartOfSpeech,
SPLIT(edges.end,"/")[5] AS endSense
MERGE (start:Concept { id:startID })
ON CREATE SET start.name = startConcept, start.language = startLanguage, start.partOfSpeech=startPartOfSpeech, start.sense=startSense
MERGE (end:Concept  {id:endID})
ON CREATE SET end.name = endConcept, end.language=endLanguage, end.partOfSpeech=endPartOfSpeech, end.sense=endSense
MERGE (start)-[r:ASSERTION]->(end)
ON CREATE SET r.type = relType, r.weight=weight, r.surfaceText=surfaceText
"""

# add track to database
addSoundCloudTrack = """
MERGE (t:Track {title:{trackTitle}})
ON CREATE SET t.id ={trackID}, t.playback_count={trackPlaybackCount}, t.permalink_url={trackPermalinkUrl}
RETURN 1
"""

# add soundcloud tag and connect to namenet5 Concepts (1:N) since tags don't have senses
addSoundCloudTag = """
MATCH (track:Track {id:{trackID}})
MERGE (tag:Tag {name:{tagName}})
MERGE (track)-[:HAS_TAG]->(tag)
WITH track, tag
MATCH (Concept:Concept {name:{conceptName}})
MERGE (tag)-[:HAS_CONCEPT]->(Concept)
RETURN track.title, tag.name, Concept.sense
"""

tracks = client.get('/tracks', order='created_at', limit=page_size, q=search_query)
for track in tracks:
	tags = list(set(shlex.split(track.tag_list.lower())))

	graph.cypher.execute(addSoundCloudTrack, trackTitle=track.title, trackID=track.id, trackPlaybackCount=track.playback_count, trackPermalinkUrl=track.permalink_url)

	for tag in tags:
		searchURL = "http://conceptnet5.media.mit.edu/data/5.4/c/en/" + tag.replace(' ','_') + "?limit=" + page_size
		searchJSON = requests.get(searchURL, headers = {"accept":"application/json"}).json()
		graph.cypher.execute(addConceptNetData, json=searchJSON)

		graph.cypher.execute(addSoundCloudTag, tagName=tag, trackID=track.id, conceptName=tag.replace(' ','_'))