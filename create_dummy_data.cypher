WITH range(1,1000000) AS counter
UNWIND counter AS counterIndex
CREATE (n:Request {requestId: toString(toInteger(rand()*900000) + 100000)})
SET n.lastUpdated = datetime({epochMillis:toInteger(rand()*(datetime().epochMillis-datetime("2000-01-01").epochMillis))+datetime("2000-01-01").epochMillis})
RETURN n.requestId, n.lastUpdated LIMIT 10



WITH range(1,1000) AS counter
UNWIND counter AS counterIndex
CREATE (n:User {userId: toString(toInteger(rand()*900000) + 100000)})
RETURN n.userId



MATCH (u:User)
WITH u
OPTIONAL MATCH (r:Request) WHERE rand() < 10.0/1000000.0
MERGE (u)-[:HAS_VIEW]->(v:View {viewTime: datetime({epochMillis:toInteger(rand()*(datetime().epochMillis-datetime("2000-01-01").epochMillis))+datetime("2000-01-01").epochMillis})})-[:VIEW_FOR]->(r)
RETURN u.userId, collect(r.requestId) LIMIT 10