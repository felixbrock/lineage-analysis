import { Document } from 'mongodb';
import { IDbConnection } from '../../domain/services/i-db';

export default class UpdateSfDataEnvRepo {

    createTempCollections = async (
        dbName: string,
        dbConnection: IDbConnection,
        callerOrgId: string
    ): Promise<void> => {
        const tempCollectionName = `temp_collection_${callerOrgId}_${dbName}`;
        const fullJoinName = `full_join_${callerOrgId}_${dbName}`;
    
        if ((await dbConnection
            .listCollections({ name: tempCollectionName })
            .toArray()).length > 0) {
          await dbConnection.collection(tempCollectionName).deleteMany({})
          .then()
          .catch((err) => {
            throw err;
          });
        } else {
          await dbConnection.createCollection(tempCollectionName)
          .then()
          .catch((err) => {
            throw err;
          });
        }
    
        if ((await dbConnection
            .listCollections({ name: fullJoinName })
            .toArray()).length > 0) {
          await dbConnection.collection(fullJoinName).deleteMany({})
          .then()
          .catch((err) => {
            throw err;
          });
        } else {
          await dbConnection.createCollection(fullJoinName)
          .then()
          .catch((err) => {
            throw err;
          });
        }
    };

    insertMany = async ( 
      dbName: string,
      dbConnection: IDbConnection,
      callerOrgId: string,
      docs: Document[]
    ): Promise<void> => {
      const tempCollectionName = `temp_collection_${callerOrgId}_${dbName}`;
      await dbConnection.collection(tempCollectionName).insertMany(docs);
    };

    fullJoin = async (
      dbName: string,
      dbConnection: IDbConnection,
      callerOrgId: string
    ): Promise<void> => {
      const tempCollectionName = `temp_collection_${callerOrgId}_${dbName}`;
      const fullJoinName = `full_join_${callerOrgId}_${dbName}`;
      const matsCollectionName = `materializations_${callerOrgId}`;

      const leftJoin = [
        {
          $lookup: {
            from: tempCollectionName,
            localField: "relation_name",
            foreignField: "concatted_name",
            as: "leftJoin"
          }
        },
        {
          $unwind: {
            path: "$leftJoin",
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $replaceRoot: {
            newRoot: {
              $mergeObjects: ["$leftJoin", "$$ROOT"]
            }
          }
        },
        {
          $merge: {
            into: fullJoinName
          } 
        },
      ];

      await dbConnection.collection(matsCollectionName).aggregate(leftJoin).toArray();

      const rightJoin = [
        {
          $lookup: {
            from: matsCollectionName,
            localField: "concatted_name",
            foreignField: "relation_name",
            as: "rightJoin"
          }
        },
        {
          $unwind: {
            path: "$rightJoin",
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $replaceRoot: {
            newRoot: {
              $mergeObjects: ["$rightJoin", "$$ROOT"]
            }
          }
        },
        {
          $merge: {
            into: fullJoinName  
          }
        }
      ];
  
      await dbConnection.collection(tempCollectionName).aggregate(rightJoin).toArray();
    };

    readDataEnv = async (
      dbName: string,
      dbConnection: IDbConnection,
      callerOrgId: string,
      lastLineageCompletedAt: string
    ): Promise<Document[]> => {
      const fullJoinName = `full_join_${callerOrgId}_${dbName}`;
      
      const pipeline = [
        {
          $match: {
            $and: [
              {
                $and: [
                  { "database_name": { $in: [dbName, null] }},
                  { "table_catalog": { $in: [dbName, null] }}
                ] 
              },
              {
                $or: [
                  {
                    $or: [
                      { "table_name": null },
                      {
                        $and: [
                          { "relation_name": null },
                          { "table_schema": { $ne: "INFORMATION_SCHEMA" }}
                        ]
                      }
                    ]
                  },
                  {
                    $expr: {
                      $gt: [
                        {
                          $divide: [
                            {
                              $subtract: [
                                { $dateFromString: { dateString: lastLineageCompletedAt } },
                                { $dateFromString: { dateString: "$last_altered" } }
                              ]
                            },
                            60000
                          ]  
                        },
                        0
                      ]
                    }
                  }
                ]
              }
            ]
          }  
        },
        {
          $project: {
            "mat_deleted_id": "$id",
            "mat_added_relation_name": "$concatted_name",
            "altered": {
              $cond: {
                if: { $and: [ { $ne: [ "$table_name", null ] }, { $ne: [ "$relation_name", null ] } ] },
                then: true,
                else: false
              }
            }
          }
        },
        {
          $group: {
            _id: "$mat_deleted_id",
            mat_deleted_id: { $first: "$mat_deleted_id" },
            mat_added_relation_name: { $first: "$mat_added_relation_name" },
            altered: { $first: "$altered" }
          }
        },
        {
          $project: {
            _id: 0,
            mat_deleted_id: 1,
            mat_added_relation_name: 1,
            altered: 1
          }
        }
      ];

      const results = await dbConnection
      .collection(fullJoinName).aggregate(pipeline).toArray();

      return results;
    };

    dropTempCollections = async (
      dbName: string,
      dbConnection: IDbConnection,
      callerOrgId: string
    ): Promise<void> => {
      const tempCollectionName = `temp_collection_${callerOrgId}_${dbName}`;
      const fullJoinName = `full_join_${callerOrgId}_${dbName}`;  

      await dbConnection.collection(tempCollectionName).drop()
      .then()
      .catch((err) => {
        throw err;
      });

      await dbConnection.collection(fullJoinName).drop()
      .then()
      .catch((err) => {
        throw err;
      });
    };
}