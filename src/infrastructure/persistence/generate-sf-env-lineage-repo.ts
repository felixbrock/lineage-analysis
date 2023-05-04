import { Document } from 'mongodb';
import { IDbConnection } from "../../domain/services/i-db";

export default class GenerateSfEnvLineageRepo {

    findBy = async (
        relationNames: string[],
        dbConnection: IDbConnection,
        callerOrgId: string
    ): Promise<Document[]> => {
        
        const results = await dbConnection
        .collection(`materializations_${callerOrgId}`)
        .find({
          'relation_name': {
            $in: relationNames.map((el) => `'${el}'`)
            // new RegExp(`^${el}$`, 'i')
          }
        })
        .project({
          id: 1,
          relation_name: 1,
        })
        .toArray();

        return results;
    };
}