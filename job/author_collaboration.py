from conf.neo4j import Neo4jDriver
import time
from conf.mysql import Cursor,Connection
import json
from utils.util import chunks
from threading import Thread

def searchCoAuthor(tx,authorId):
    res = tx.run('''MATCH (a1:Author{authorId:$authorId}) 
            CALL apoc.path.spanningTree(a1, {minLevel:1,maxLevel:2,relationshipFilter:"Collaboration"}) YIELD path 
            WITH last(nodes(path)) as p,length(path) as numberOfHops 
            order by p.authorId 
            WITH collect(p) as nodes 
            RETURN nodes
            ''',authorId=authorId)
    return res.single()[0]

def computeExistedCollaborationDistance(start,end):
    intersection_sql = '''
    SELECT COUNT(DISTINCT a1.article_id) FROM acmieee.author_article a1,acmieee.author_article a2
    WHERE a1.author_id = %s AND a2.author_id = %s AND a1.article_id = a2.article_id
    '''
    union_sql = '''
    SELECT COUNT(DISTINCT article_id) FROM acmieee.author_article 
    WHERE author_id = %s OR author_id = %s
    '''

    Cursor.execute(intersection_sql,(str(start),str(end)))
    intersection_num = Cursor.fetchone()[0]

    Cursor.execute(union_sql,(str(start),str(end)))
    union_num = Cursor.fetchone()[0]

    if union_num==0:
        return 1

    return 1-intersection_num/union_num

def computeAffiliationDistance(start,end):
    intersection_sql = '''
    SELECT COUNT(DISTINCT a1.affiliation_id) FROM acmieee.affiliation_author a1,acmieee.affiliation_author a2
    WHERE a1.author_id = %s AND a2.author_id = %s AND a1.affiliation_id = a2.affiliation_id
    '''
    union_sql = '''
    SELECT COUNT(DISTINCT affiliation_id) FROM acmieee.affiliation_author 
    WHERE author_id = %s OR author_id = %s
    '''

    Cursor.execute(intersection_sql, (str(start), str(end)))
    intersection_num = Cursor.fetchone()[0]

    Cursor.execute(union_sql, (str(start), str(end)))
    union_num = Cursor.fetchone()[0]

    if union_num == 0:
        return 1

    return 1 - intersection_num / union_num

def computeDirectionDistance(start,end):
    intersection_sql = '''
    SELECT group_concat(DISTINCT a1.keyword_desc separator "\t") FROM acmieee.keyword_author a1,acmieee.keyword_author a2
    WHERE a1.author_id = %s AND a2.author_id = %s AND a1.keyword_id = a2.keyword_id
    '''
    union_sql = '''
    SELECT COUNT(DISTINCT keyword_id) FROM acmieee.keyword_author 
    WHERE author_id = %s OR author_id = %s
    '''

    Cursor.execute(intersection_sql, (str(start), str(end)))
    intersection = Cursor.fetchone()[0]
    intersection_num = 0
    predictDirections = []
    if intersection:
        predictDirections = intersection.split("\t")
        intersection_num = len(predictDirections)
    Cursor.execute(union_sql, (str(start), str(end)))
    union_num = Cursor.fetchone()[0]

    if union_num == 0:
        return 1

    return [1 - intersection_num / union_num,predictDirections]



def computeJaccrdDistance(start,end):
    existedDistance = 0.5*computeExistedCollaborationDistance(start,end)
    affiliationDistance = 0.2*computeAffiliationDistance(start,end)
    directions = computeDirectionDistance(start,end)
    directionDistance = 0.3*directions[0]
    predictDirections = directions[1]
    distance = existedDistance+affiliationDistance+directionDistance
    return [distance,predictDirections]



def update_author_collaboration_job():
    sql = 'SELECT id FROM author'
    Cursor.execute(sql)
    author_list = list(map(lambda x:x[0],list(Cursor.fetchall())))
    update_sql = '''
        INSERT INTO author_collaboration(start_id,end_id,distance,predict_collaboration)
        VALUES (%s,%s,%s,%s)
    '''
    author_collaboration_list = []
    wfile = open("/Users/Karl/Desktop/SoftwareExercise/authorCollaboration.txt", "a+", encoding="utf-8")
    for authors in chunks(author_list,500):
        start_time = time.time()
        for author in authors:
            with Neo4jDriver.session() as session:
                res = session.read_transaction(searchCoAuthor, author)
                # data = []
                # for record in res:
                #     data.append(record["authorId"])
                # author_collaboration_dict[author]=data
    #     wfile.write(json.dumps(author_collaboration_dict, indent=4))
    #     author_collaboration_dict.clear()
    #     end_time = time.time()
    #     duration = end_time - start_time
    #     print('update_author_collaboration_job runtime is:{0:.3f}s'.format(duration))
    # wfile.close()

                for coAuthor in res:
                    jaccrdDistance = computeJaccrdDistance(author, coAuthor["authorId"])
                    print((author, coAuthor["authorId"], round(jaccrdDistance[0], 2),
                           json.dumps(jaccrdDistance[1])))
                    author_collaboration_list.append((author, coAuthor["authorId"], round(jaccrdDistance[0], 2),
                                                      json.dumps(jaccrdDistance[1])))
        try:
            Cursor.executemany(update_sql,author_collaboration_list)
            Connection.commit();
        except Exception as e:
            print(e)
            Connection.rollback()
        end_time = time.time()
        duration = end_time - start_time
        print('update_author_collaboration_job 500 runtime is:{0:.3f}s'.format(duration))
        time.sleep(1)


if __name__ == "__main__":
    start_time = time.time()
    update_author_collaboration_job()
    end_time = time.time()
    duration = end_time - start_time
    print('update_author_collaboration_job runtime is:{0:.3f}s'.format(duration))