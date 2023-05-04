import { Document } from "mongodb";
import { IDbConnection } from "../../domain/services/i-db";

export default class GetSfExternalDataEnvRepo {

    allRelations = async (
        dbConnection: IDbConnection,
        callerOrgId: string
    ): Promise<Document[]> => {
        
        const results = await dbConnection
        .collection(`materializations_${callerOrgId}`)
        .find({})
        .project({ id: 1, relation_name: 1 })
        .toArray();

        return results;
    };
}