UNWIND $batch AS batchItem
MATCH (r:Request)
WHERE r.requestId = batchItem.requestId
SET r.viewedByUserIds = batchItem.viewedByUserIds
RETURN r.requestId AS requestId, size(batchItem.viewedByUserIds) AS numberOfviewedByUserIds