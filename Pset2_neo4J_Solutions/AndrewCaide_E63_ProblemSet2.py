# -*- coding: utf-8 -*-
"""
# ANDREW CAIDE
# SATURDAY, SEP 16 2017
# E-63, Big Data Analytics
# Problem Set 2
#

Please, describe every step of your work and present all 
intermediate and final results in a Word document. 
Please, copy past text version of all essential command and snippets of 
results into the Word document with explanations of the purpose of those commands. 
We cannot retype text that is in JPG images. 
Please, always submit a separate copy of the original, working scripts and/or 
class files you used. Sometimes we need to run your code and retyping is too costly. 
Please include in your MS Word document only relevant portions of the 
console output or output files. Sometime either console output or the result 
file is too long and including it into the MS Word document makes that document 
too hard to read. PLEASE DO NOT EMBED files into your MS Word document. 
For issues and comments visit the class Discussion Board on Piazza. 

You can do most of this assignment in Python, Java, R, Scala or any other 
language of your convenience.

###
Problem 1. 

The following is the content of Movies database. 
Bring that database into Neo4J using curl.  #   (--only?)

#           This code is not great btw... You can shorten it to 1/3 lines

CREATE (matrix2:Movie { title : 'The Matrix Reloaded', year : '2003-05-07' ) return id(matrix2)
CREATE (matrix3:Movie { title : 'The Matrix Revolutions', year : '2003-10-27' ) return id(matrix3)
CREATE (keanu:Actor { name:'Keanu Reeves' ) return id(Keanu)
CREATE (laurence:Actor { name:'Laurence Fishburne' )
CREATE (carrieanne:Actor { name:'Carrie-Anne Moss' )
CREATE (keanu)-[:ACTS_IN { role : 'Neo' ]->(matrix1)
CREATE (keanu)-[:ACTS_IN { role : 'Neo' ]->(matrix2)
CREATE (keanu)-[:ACTS_IN { role : 'Neo' ]->(matrix3)
CREATE (laurence)-[:ACTS_IN { role : 'Morpheus' ]->(matrix1)
CREATE (laurence)-[:ACTS_IN { role : 'Morpheus' ]->(matrix2)
CREATE (laurence)-[:ACTS_IN { role : 'Morpheus' ]->(matrix3)
CREATE (carrieanne)-[:ACTS_IN { role : 'Trinity' ]->(matrix1)
CREATE (carrieanne)-[:ACTS_IN { role : 'Trinity' ]->(matrix2)
CREATE (carrieanne)-[:ACTS_IN { role : 'Trinity' ]->(matrix3)


"""
# Set up libraries
from neo4j.v1 import GraphDatabase
import subprocess

"""
###     Solution
I wasn't sure what this question was asking, more specifically, how curl worked.
My initial attempt was design a curl function that would run a python script, but I thought
that was a little easy..? 
Because it asked for only curl, I designed the .json script below. It's rather 
heavy for a terminal command, so I wrote it to a file .(txt) and used curl to 
read it into the db.

#       cURL command

curl -H 'Content-Type: application/json' -X POST -T /Users/.../toCurl_Problem1.txt -u 
neo4j:neo4jneo4j http://localhost:7474/db/data/transaction/commit

#
Which was pointing into a textfile somewhere in my hd pointed to the following .txt file:

#       File contents

{"statements":
    [
        {"statement": "CREATE(matrix1:Movie {title:'The Matrix'}) "},
        {"statement": "CREATE(matrix2:Movie { title : 'The Matrix Reloaded', year : '2003-05-07' }) "},
        {"statement": "CREATE(matrix3:Movie { title : 'The Matrix Revolutions', year : '2003-10-27'})"},
        {"statement": "CREATE(keanu:Actor {name:'Keanu Reeves', id:'keanu'}) "},
        {"statement": "CREATE(carrie:Actor {name:'Carrie-Anne Moss',id:'carrie'}) "},
        {"statement": "CREATE(laurence:Actor {name:'Laurence Fishburne', id:'laurence'}) "},
        {"statement": "MATCH (p:Actor{name:'Keanu Reeves'}), (m:Movie) CREATE (p)-[:ACTS_IN {role:'Neo'}]->(m)"},
        {"statement": "MATCH (p:Actor{name:'Laurence Fishburne'}), (m:Movie) CREATE (p)-[:ACTS_IN {role:'Morpheus'}]->(m)" },
        {"statement": "MATCH (p:Actor{name:'Carrie-Anne Moss'}), (m:Movie) CREATE (p)-[:ACTS_IN {role:'Trinity'}]->(m)"},   
  ]
}
    
#

This solves the question. But because I did it anyways, the following code, minus
a call to subprocess (or whatever macs use) to auto run it. 
   
 
##      SOLUTION
"""

#Access server
uri = "bolt://127.0.0.1:7687"
driver = GraphDatabase.driver(uri,auth=('neo4j','neo4jneo4j'))

#Constuct cyphers in a list
cypher = [
    "CREATE (matrix1:Movie { title : 'The Matrix', year : '1999-03-31', id : 'matrix1' })",
    "CREATE (matrix2:Movie { title : 'The Matrix Reloaded', year : '2003-05-07' , id : 'matrix2' })",
    "CREATE (matrix3:Movie { title : 'The Matrix Revolutions', year : '2003-10-27' , id : 'matrix3' })",
    
    "CREATE (keanu:Actor { name:'Keanu Reeves', id: 'keanu' })",
    "CREATE (laurence:Actor { name:'Laurence Fishburne', id: 'laurence'})",
    "CREATE (carrieanne:Actor { name:'Carrie-Anne Moss' , id: 'carrieanne'})",
    
    "MATCH (p:Actor{name:'Keanu Reeves'}), (m:Movie) CREATE (p)-[:ACTS_IN {role:'Neo'}]->(m)",
    "MATCH (p:Actor{name:'Laurence Fishburne'}), (m:Movie) CREATE (p)-[:ACTS_IN {role:'Morpheus'}]->(m)",
    "MATCH (p:Actor{name:'Carrie-Anne Moss'}), (m:Movie) CREATE (p)-[:ACTS_IN {role:'Trinity'}]->(m)"
        ]

#Open up a session, and toss everything in one by one
with driver.session() as session:  
    for q in cypher:
        session.run(q)
    session.close()
    
"""
Question!! I have a more elegant solution, but it only works in the neo4j console, not python.
What gives? (From what I can guess, is that these lines have to be processed simultaniously,
but from my test I cant do simultanious passes...)
Note: First two tripplet commands are identical to previous cypher. 3rd block is culprit


CREATE (matrix1:Movie {title:'The Matrix'}) 
CREATE (matrix2:Movie { title : 'The Matrix Reloaded', year : '2003-05-07' }) 
CREATE (matrix3:Movie { title : 'The Matrix Revolutions', year : '2003-10-27' }) 

CREATE (keanu:Person {name:'Keanu Reeves', id:'keanu'}) 
CREATE (carrie:Person {name:'Carrie-Anne Moss',id:'carrie'}) 
CREATE (laurence:Person {name:'Laurence Fishburne', id:'laurence'}) 

WITH keanu, carrie, laurence
MATCH (m:Movie)                         
CREATE (keanu)-[:ACTS_IN {role:'Neo'}]->(m) 
CREATE (carrie)-[:ACTS_IN {role:'Trinity'}]->(m) 
CREATE (laurence)-[:ACTS_IN {role:'Morpheus'}]->(m)
    

error: 
    CypherSyntaxError: Variable `keanu` not defined (line 1, column 6 (offset: 5))
"WITH keanu, carrie, laurence MATCH (m:Movie) CREATE (keanu)-[:ACTS_IN {role:'Neo'}]->(m) 
CREATE (carrie)-[:ACTS_IN {role:'Trinity'}]->(m) CREATE (laurence)-[:ACTS_IN {role:'Morpheus'}]->(m)"

"""



#
# ANDREW CAIDE
# SATURDAY, SEP 16 2017
# E-63, Big Data Analytics
# Problem Set 2
#




"""
Problem 2. 

Keanu Reeves acted in the movie “John Wick” which is not in the database. 
That movie was directed by Chad Stahelski and David Leitch. 
Cast of the movie included William Dafoe and Michael Nyquist. 

a) Demonstrate that you have successfully brought data about John Wick movie into the database. 
You can use Cypher Browser or any other means. 

b) Delete above movie and all the cast except Keanu Reeves.

(15%)


a)
    
 After that nightmare, I just wrote another cURL->.json and read it on the cypher browser...
 
     cURL: 
     curl -H 'Content-Type: application/json' -X POST -T /Users/.../toCurl_Problem2.txt -u 
     neo4j:neo4jneo4j http://localhost:7474/db/data/transaction/commit
     
    The .txt file (toCurl_Problem2.txt): --where can i find a tool? my searches were bad
        
{"statements":
    [
        {"statement": "CREATE(johnwick:Movie { title : 'John Wick'}) "},
        {"statement": "MATCH (p:Actor{name:'Keanu Reeves'}), (m:Movie{title: 'John Wick'}) CREATE (p)-[:ACTS_IN]->(m)"},
        {"statement": "CREATE(chad:Director {name:'Chad Stahelski', id:'chad'}) "},
        {"statement": "CREATE(david:Director {name:'David Leitch', id:'david'}) "},
        {"statement": "MATCH (d:Director), (m:Movie{title: 'John Wick'}) CREATE (d)-[:DIRECTED]->(m) "},      
        {"statement": "CREATE(will:Actor {name:'William Dafoe', id:'will'}) "},
        {"statement": "CREATE(mike:Actor {name:'Michael Nyquist', id:'mike'}) "},
        {"statement": "MATCH (p:Actor{name:'William Dafoe'}), (m:Movie{title: 'John Wick'}) CREATE (p)-[:ACTS_IN]->(m) "},
        {"statement": "MATCH (p:Actor{name:'Michael Nyquist'}), (m:Movie{title: 'John Wick'}) CREATE (p)-[:ACTS_IN]->(m)"}
      ]
}

Next just read the results from 'match(n) return n' via console. 

b) 
    Detatch all relationships and then delete everything that isn't keanu in the aforementioned movie

Neo4J Code:
    
    match(movie:Movie{title:"John Wick"}) -- (related) where not (related.name="Keanu Reeves") detach delete movie, related
    
Here we match everybody that is related (in any way) to the john wick movie, EXCEPT keanu, delete them

"""


#
# ANDREW CAIDE
# SATURDAY, SEP 16 2017
# E-63, Big Data Analytics
# Problem Set 2
#



"""
Problem 3. 

Add all the actors and the roles they played in this movie “John Wick” to the database using JAVA REST API or 
some other APIs for Neo4J in a language of your choice (not Curl). 
Demonstrate that you have successfully brought data about John Wick movie into the database. 
You can use Cypher Browser or any other means.
(15%)
"""
# Same thing as above but done with python cyphers - continued from problem 1's graph

cypher_3 = [
      "CREATE(johnwick:Movie { title : 'John Wick'}) ",
      "MATCH (p:Actor{name:'Keanu Reeves'}), (m:Movie{title: 'John Wick'}) CREATE (p)-[:ACTS_IN]->(m)",
      "CREATE(chad:Director {name:'Chad Stahelski', id:'chad'}) ",
      "CREATE(david:Director {name:'David Leitch', id:'david'}) ",
      "MATCH (d:Director), (m:Movie{title: 'John Wick'}) CREATE (d)-[:DIRECTED]->(m) ",      
      "CREATE(will:Actor {name:'William Dafoe', id:'will'}) ",
      "CREATE(mike:Actor {name:'Michael Nyquist', id:'mike'}) ",
      "MATCH (p:Actor{name:'William Dafoe'}), (m:Movie{title: 'John Wick'}) CREATE (p)-[:ACTS_IN]->(m) ",
      "MATCH (p:Actor{name:'Michael Nyquist'}), (m:Movie{title: 'John Wick'}) CREATE (p)-[:ACTS_IN]->(m)"
      
            ]

with driver.session() as session:  
    for q in cypher_3:
        result = session.run(q)
        for record in result:
            print("%s",record)
    session.close()

# PROOF

## To demonstrate a successful implementation, I ran two cyphers on the client:
    
    # First I checked to see it was implimented 
    # match(m:Movie{title:"John Wick"})-[r]-(p) return m as movie, p as people, r as relationship
    # Secondly I checked to make sure it wasn't the only thing (I should have matrix data here)
    # ...just so I know I'm not fooling myself
    # match(m) return m
     
#############################################   
    
#
# ANDREW CAIDE
# SATURDAY, SEP 16 2017
# E-63, Big Data Analytics
# Problem Set 2
#
    
    
"""
Problem 4. 
Find a list of actors playing in movies in which Keanu Reeves played. 
Find directors of movies in which K. Reeves played. 
Please use any language of your convenience (Java, Python, C#, R, curl). 
Verify your results using Cypher queries in Cypher Browser
(15%)

I created a python cypher, rather straightforward
First I create a dummy node to make sure I'm not pulling in junk
MIB is not junk, to clarify

The fourth/fifth line of the cypher is the solution:
    Find the movies in which Keanu acts in, select all of the names/directors
    There's a funky issue with my db, but I'm getting doubles/tripples, so lets select distinct values
    and pool it into a list for compactness
"""   
#  Cyphers: Using the previous maps

cypher_4 = [ 
       "CREATE(will:Actor {name:'Will Smith', id:'willsmith'}) ",
       "CREATE(mib:Movie { title : 'Men in Black'}) ",
       "MATCH (p:Actor{name:'Will Smith'}), (m:Movie{title: 'Men in Black'}) CREATE (p)-[:ACTS_IN]->(m) ",
       "MATCH (p:Actor{name:'Keanu Reeves'})-[:ACTS_IN]-> (m:Movie) MATCH (a:Actor) -[:ACTS_IN] ->(m) Return collect(distinct a.name) as name",
       "MATCH (p:Actor{name:'Keanu Reeves'})-[:ACTS_IN]-> (m:Movie) MATCH (d:Director) -[:DIRECTED] ->(m) Return collect(distinct d.name) as name"
       ]

#
n=0
with driver.session() as session:  
    for q in cypher_4:
        n += 1
        result = session.run(q)
        if n>3:
            if n == 4:
                print("Actors:")
            if n == 5:
                print("Directors:")
            for record in result:
                print("%s" % record["name"])
    session.close() 
    
#############################################


#
# ANDREW CAIDE
# SATURDAY, SEP 16 2017
# E-63, Big Data Analytics
# Problem Set 2
#


"""
Problem 5
Find a way to export data from Neo4j into a set of CSV files. 
Delete your database and demonstrate that you can recreate the database by loading those CSV files. 
Please use any programming language of your convenience: Java, Python, R, C# or Scala.
(20%)


I could not find a way to export the csv through python nor curl. I did so via
the following neo4j command and the export button:

    #optional match because not all movies had a director

    match(m:Movie)-[r]-(a:Actor) 
    optional match (d:Director) -- (m) 
    return  a.name as Actor, type(r) as Relationship, m.title as Title, m.year as Year, d.name as Director

[export csv]


Now, to upload this data set, I had to use two queries: One to submit movie and actor data 
with relationships, another to submit director data and their relationships. Unsure how to do this in one go

1)
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///P2Q5.csv" AS line FIELDTERMINATOR ","

MERGE (a:Actor{Name:line.Actor})
ON CREATE SET a.name = line.Actor
MERGE (m:Movie{Title:line.Title})
ON CREATE SET m.title = line.Title

MERGE ((a)-[r:ACTED_IN{R:line.Relationship}]->(m))

2)
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///P2Q5.csv" AS line FIELDTERMINATOR ","
WITH line where line.Director is not null

MERGE (m:Movie{Title:line.Title})
ON CREATE SET m.title = line.Title

MERGE (d:Director{Name:line.Director})
ON CREATE SET d.Name = line.Director
MERGE (d)-[:DIRECTED]->(m)

There is a small issue in my map where director names are not displaying. I can't 
quite figure this one out. 
"""

"""
Problem 6. 
Find a way to use Arrow Tool (http://www.apcjones.com) to paint a relationship between a dog 
and his owner who live in New York and walk through the Central Park on Sunday afternoon. 
Add Labels and necessary properties to all nodes and relationships.
 xport your graph in Cypher format and then adjust (if necessary) generated Cypher so that 
you can create that graph in Neo4J database. 
Verify that your graph is indeed created using Cypher Browser.
(15%)

This was a little tricky without any explanation about the tool. But after some
testing I realized the outside-rim of the nodes glow purple if your cursor is
hovering around it, and that's how edges are formed 

After designing the graph, I got the following cypher

CREATE 
  (`1` :Owner {Name:'Frank',Lives:'New York',Type:'Owner'}) ,
  (`2` :`New York` {Type:'City'}) ,
  (`3` :`Central Park` {Type:'Location'}) ,
  (`4` :Dog {Name:'Scooby',Type:'Dog',Lives:'New York'}) ,
  (`2`)-[:`Contains`]->(`3`),
  (`1`)-[:`Owner`]->(`4`),
  (`4`)-[:`Loves`]->(`3`),
  (`4`)-[:`Habitat`]->(`2`),
  (`1`)-[:`Habitat`]->(`2`),
  (`4`)-[:`Pet`]->(`1`)
  
  This can be copy-pasted into the cypher console and executed as is! 
  But it does not contain the full level of detail I created with the arrow tool, especially the
  edges/vectors. Below is the adjusted cypher
  
CREATE 
  (`1` :Owner {Name:'Frank',Lives:'New York',Type:'Owner'}) ,
  (`2` :`New York` {Type:'City'}) ,
  (`3` :`Central Park` {Type:'Location'}) ,
  (`4` :Dog {Name:'Scooby',Type:'Dog',Lives:'New York'}) ,
  (`2`)-[:`Contains`{Relationship: 'Location within'}]->(`3`),
  (`1`)-[:`Owner`{Relationship: 'Best Friends', Actions: 'Takes for walks on Sunday Afternoon'}]->(`4`),
  (`4`)-[:`Loves`{Relationship: 'Favorite Location',Visits: 'Sunday Afternoon'}]->(`3`),
  (`4`)-[:`Habitat`{Relationship: 'Lives in'}]->(`2`),
  (`1`)-[:`Habitat`{Relationship: 'Lives in'}]->(`2`),
  (`4`)-[:`Pet`{Relationship: 'Best Friends',Actions: 'Barks'}]->(`1`)
  
  It can be confirmed with 
  
  MATCH(n) RETURN n
  
"""
# ANDREW CAIDE
# SATURDAY, SEP 16 2017
# E-63, Big Data Analytics
# Problem Set 2
