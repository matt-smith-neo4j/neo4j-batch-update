UNWIND $batch AS batchItem
MATCH (r:Request)<-[:VIEW_FOR]-(v:View)<-[:HAS_VIEW]-(u:User)
WHERE r.requestId = batchItem.requestId AND v.viewTime >= r.lastUpdated
RETURN r.requestId AS requestId, collect(u.userId) AS viewedByUserIds