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

        try {
          const tempCollectionExists = await dbConnection
            .listCollections({ name: tempCollectionName })
            .toArray();

          if (tempCollectionExists.length > 0) {
            await dbConnection.collection(tempCollectionName).deleteMany({});
          } else {
            await dbConnection.createCollection(tempCollectionName);
          }

          const fullJoinCollectionExists = await dbConnection
            .listCollections({ name: fullJoinName })
            .toArray();
          
          if (fullJoinCollectionExists.length > 0) {
            await dbConnection.collection(fullJoinName).deleteMany({});
          } else {
            await dbConnection.createCollection(fullJoinName);
          }

        } catch (err) {
          console.log(err);
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
          $project: {
            "id": { $cond: [{ $eq: [{$type: "$id"}, "missing"]}, null, "$id" ]},
            "database_name": { $cond: [{ $eq: [{$type: "$database_name"}, "missing"]}, null, "$database_name" ]},
            "table_catalog": { $cond: [{ $eq: [{$type: "$table_catalog"}, "missing"]}, null, "$table_catalog" ]},
            "table_name": { $cond: [{ $eq: [{$type: "$table_name"}, "missing"]}, null, "$table_name" ] },
            "relation_name": { $cond: [{ $eq: [{$type: "$relation_name"}, "missing"]}, null, "$relation_name" ]},
            "table_schema": { $cond: [{ $eq: [{$type: "$table_schema"}, "missing"]}, null, "$table_schema" ]},
            "last_altered": { $cond: [{ $eq: [{$type: "$last_altered"}, "missing"]}, null, "$last_altered" ] },
            "concatted_name": { $cond: [{ $eq: [{$type: "$concatted_name"}, "missing"]}, null, "$concatted_name" ] }
          }
        },
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

      try {
        await dbConnection.collection(tempCollectionName).drop();
        await dbConnection.collection(fullJoinName).drop();
      } catch (err) {
        console.log(err);
      }
    };
}