import { Document } from 'mongodb';
import {
  Lineage,
  LineageProps,
  parseLineageCreationState,
} from '../../domain/entities/lineage';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { ColumnDefinition, Query } from './shared/base-sf-repo';
import {
  ILineageRepo,
  LineageQueryDto,
  LineageUpdateDto,
} from '../../domain/lineage/i-lineage-repo';
import BaseAuth from '../../domain/services/base-auth';
import { IDbConnection } from '../../domain/services/i-db';

export default class LineageRepo
  extends BaseSfRepo<Lineage, LineageProps, LineageQueryDto, LineageUpdateDto>
  implements ILineageRepo
{
  readonly matName = 'lineage_snapshots';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'created_at', nullable: false },
    { name: 'db_covered_names', selectType: 'parse_json', nullable: false },
    { name: 'diff', nullable: true },
    { name: 'creation_state', nullable: false },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (document: Document): LineageProps => {
    const {
      id,
      created_at: createdAt,
      db_covered_names: dbCoveredNames,
      diff,
      creation_state: creationState,
    } = document;

    const createdAtDate = new Date(createdAt);

    const dbCoveredNamesArray = JSON.parse(dbCoveredNames);

    if (
      typeof id !== 'string' ||
      !(createdAtDate instanceof Date) ||
      !LineageRepo.isStringArray(dbCoveredNamesArray) ||
      !LineageRepo.isOptionalOfType<string>(diff, 'string')
    )
      throw new Error(
        'Retrieved unexpected lineage field types from persistence'
      );

    return {
      id,
      creationState: parseLineageCreationState(creationState),
      createdAt: createdAtDate.toISOString(),
      dbCoveredNames: dbCoveredNamesArray,
      diff,
    };
  };

  findLatest = async (
    filter: { tolerateIncomplete: boolean; minuteTolerance?: number },
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<Lineage | undefined> => {
    try {  
      const query: any = { creation_state: 'completed' };

      if (filter.tolerateIncomplete) {
        const incompleteQuery: any = {
          creation_state: { $ne: 'complete' },
        };

        if (filter.minuteTolerance) {
          incompleteQuery.created_at = 
          {
            $gte: new Date(Date.now() - (filter.minuteTolerance * 1000 * 60)),
          };
        }

        query.$or = [incompleteQuery, { creation_state: 'completed' }];
      }

      const result = await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
      .find(query)
      .sort({ created_at: -1 })
      .limit(1)
      .toArray();

      if (!result.length) return undefined;
      if (result.length > 1)
          throw new Error(`Multiple lineage entities with id found`);

      return !result.length ? undefined : this.toEntity(this.buildEntityProps(result[0]));
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  getValues = (entity: Lineage): (string | number)[] => [
    entity.id,
    entity.createdAt,
    JSON.stringify(entity.dbCoveredNames),
    entity.diff || 'null',
    entity.creationState,
  ];

  buildFindByQuery(dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${JSON.stringify(dto)}]`
    );
  }

  buildUpdateQuery(id: string, dto: LineageUpdateDto): Query {
    const colDefinitions: ColumnDefinition[] = [this.getDefinition('id')];
    const values = [id];

    if (dto.creationState) {
      colDefinitions.push(this.getDefinition('creation_state'));
      values.push(dto.creationState);
    }

    if (dto.dbCoveredNames) {
      colDefinitions.push(this.getDefinition('db_covered_names'));
      values.push(JSON.stringify(dto.dbCoveredNames));
    }

    if (dto.diff) {
      colDefinitions.push(this.getDefinition('diff'));
      values.push(dto.diff);
    }

    return { values, colDefinitions };
  }

  toEntity = (lineageProperties: LineageProps): Lineage =>
    Lineage.build(lineageProperties);
}
