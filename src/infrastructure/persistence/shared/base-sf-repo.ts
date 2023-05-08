import { Blob } from 'node:buffer';
import BaseAuth from '../../../domain/services/base-auth';
import { IServiceRepo } from '../../../domain/services/i-service-repo';
import {
  Bind,
  Binds,
  IConnectionPool,
  SnowflakeEntity,
} from '../../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../../domain/snowflake-api/query-snowflake';
import { appConfig } from '../../../config';
import { IDbConnection } from '../../../domain/services/i-db';

export interface ColumnDefinition {
  name: string;
  selectType?: SelectType;
  nullable: boolean;
}
export interface Query {
  text?: string;
  values: (string | number | boolean)[];
  colDefinitions?: ColumnDefinition[];
  filter?: any
}

export type SelectType = 'parse_json';

export default abstract class BaseSfRepo<
  Entity extends { id: string },
  EntityProps,
  QueryDto extends object | undefined,
  UpdateDto extends object | undefined
> implements IServiceRepo<Entity, QueryDto, UpdateDto>
{
  protected abstract readonly matName: string;

  protected abstract readonly colDefinitions: ColumnDefinition[];

  protected readonly querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.querySnowflake = querySnowflake;
  }

  #relationPath = `${appConfig.snowflake.databaseName}.${appConfig.snowflake.schemaName}`;

  #getInsertQueryText = (
    matName: string,
    columnDefinitions: ColumnDefinition[],
    rows: unknown[]
  ): string => `
        insert into ${this.#relationPath}.${matName}(${columnDefinitions
    .map((el) => el.name)
    .join(', ')})
        select ${columnDefinitions
          .map((el, index) => {
            const value = el.selectType
              ? `${el.selectType}($${index + 1})`
              : `$${index + 1}`;
            return el.nullable ? `nullif(${value}::string, 'null')` : value;
          })
          .join(', ')}
        from values ${rows.join(', ')};
        `;

  protected getUpdateQueryText = (
    matName: string,
    colNames: ColumnDefinition[],
    rows: string[]
  ): string => `
          merge into ${this.#relationPath}.${matName} target
          using (
          select ${colNames
            .map((el, index) => {
              const value = el.selectType
                ? `${el.selectType}($${index + 1})`
                : `$${index + 1}`;
              return el.nullable
                ? `nullif(${value}::string, 'null') as ${el.name}`
                : `${value} as ${el.name}`;
            })
            .join(', ')}
          from values ${rows.join(', ')}) as source
          on source.id = target.id
        when matched then update set ${colNames
          .map((el) => `target.${el.name} = source.${el.name}`)
          .join(', ')};
          `;

  findOne = async (
    id: string,
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<Entity | undefined> => {
    try {
      const result = await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
      .findOne({ id });

      if (!result) return undefined;

      return this.toEntity(this.buildEntityProps(result)); 
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  protected abstract buildFindByQuery(dto: QueryDto): Query;

  findBy = async (
    queryDto: QueryDto,
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<Entity[]> => {
    try {
      if (!queryDto || !Object.keys(queryDto).length)
        return await this.all(auth, dbConnection);
      
        const query = this.buildFindByQuery(queryDto);

        const result = await dbConnection
        .collection(`${this.matName}_${auth.callerOrgId}`)
        .find(query.filter).toArray();
  
        if (!result) return [];

        return result.map((el) => this.toEntity(this.buildEntityProps(el)));
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findByCustom = async (
    query: { text: string; binds: Binds },
    auth: BaseAuth,
    connPool: IConnectionPool
  ): Promise<Entity[]> => {
    try {
      const result = await this.querySnowflake.execute(
        { queryText: query.text, binds: query.binds },
        auth,
        connPool
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return result.value.map((el) => this.toEntity(this.buildEntityProps(el)));
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  all = async (
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<Entity[]> => {
    try {
      const result = await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
      .find({}).toArray();

      if (!result) return [];

      return result.map((el) => this.toEntity(this.buildEntityProps(el)));
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  protected abstract getValues(entity: Entity): (string | number | boolean)[];

  insertOne = async (
    entity: Entity,
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<string> => {
    try {
			const row = this.getValues(entity);

			const document: any = {};
			this.colDefinitions.forEach((column, index) => {
        const value = row[index];
				document[column.name] = column.nullable && value === 'null' ? null : value;
			});

			const result = await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
      .insertOne(document);

      if (!result.acknowledged) throw new Error('Insert not acknowledged');

      return entity.id;
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #splitQuery = (
    baseQueryText: string,
    leadingConstantBinds: Bind[],
    arrayBasedBinds: Bind[],
    tailingConstantBinds: Bind[],
    arrayBasedBindsPlaceholder: string
  ): { queryText: string; binds: Bind[] }[] => {
    const byteToMBDivisor = 1000000;
    const querySizeOffset = 0.1;

    const baseQueryTextMBSize =
      new Blob([baseQueryText]).size / byteToMBDivisor;
    const constantBindsMBSize =
      new Blob([
        JSON.stringify(leadingConstantBinds.concat(tailingConstantBinds)),
      ]).size / byteToMBDivisor;

    const bindsMBSize =
      new Blob([JSON.stringify(arrayBasedBinds)]).size / byteToMBDivisor;
    const bindingMBSize =
      new Blob([arrayBasedBinds.map(() => '?, ').join(',')]).size /
      byteToMBDivisor;

    // in MB (subtracting offset)
    const maxSize = 1 * (1 - querySizeOffset);
    const maxBindingSequenceMBSize =
      maxSize - baseQueryTextMBSize - constantBindsMBSize;

    const numSequences = Math.ceil(
      (bindsMBSize + bindingMBSize) / maxBindingSequenceMBSize
    );
    const numElementsPerSequence = Math.ceil(
      arrayBasedBinds.length / numSequences
    );

    const queries: { queryText: string; binds: Bind[] }[] = [];
    for (let i = 0; i < arrayBasedBinds.length; i += numElementsPerSequence) {
      const chunk = arrayBasedBinds.slice(i, i + numElementsPerSequence);

      const queryText = baseQueryText.replace(
        arrayBasedBindsPlaceholder,
        chunk.map(() => '?').join(',')
      );

      const binds = leadingConstantBinds.concat(chunk, tailingConstantBinds);

      queries.push({ queryText, binds });
    }

    return queries;
  };

  #splitBinds = (queryTextSize: number, binds: Binds): Binds[] => {
    // todo - Upload as file and then copy into table
    const byteToMBDivisor = 1000000;
    const maxQueryMBSize = 1;
    const querySizeOffset = 0.1;
    const maxQueryTextMBSize = 0.2;

    const queryTextMBSize = queryTextSize / byteToMBDivisor;
    const bindsSize = new Blob([JSON.stringify(binds)]).size;
    const bindsMBSize = bindsSize / byteToMBDivisor;

    if (queryTextMBSize + bindsMBSize < maxQueryMBSize * (1 - querySizeOffset))
      return [binds];
    if (queryTextMBSize > maxQueryTextMBSize)
      throw new Error('Query text size too large. Implement file upload');

    // in MB (subtracting offset)
    const maxSize = 1 * (1 - querySizeOffset);
    const maxBindsSequenceMBSize = maxSize - queryTextMBSize;

    const numSequences = Math.ceil(bindsMBSize / maxBindsSequenceMBSize);
    const numElementsPerSequence = Math.ceil(binds.length / numSequences);

    const res: Binds[] = [];
    for (let i = 0; i < binds.length; i += numElementsPerSequence) {
      const chunk = binds.slice(i, i + numElementsPerSequence);
      res.push(chunk);
    }

    return res;
  };

  insertMany = async (
    entities: Entity[],
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<string[]> => {
    try {
      if (entities.length === 0) return [];

      const rows = entities.map((entity) => this.getValues(entity));

			const documents = rows.map(row => {
				const document: any = {};
				this.colDefinitions.forEach((column, index) => {
					const value = row[index];
					document[column.name] = column.nullable && value === 'null' ? null : value;
				});

				return document;
			});

			const results = await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
      .insertMany(documents);

      if (results.insertedCount !== documents.length) {
        throw new Error('Failed to insert all documents successfully');
      }

      return entities.map((el) => el.id);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  getDefinition = (name: string): ColumnDefinition => {
    const def = this.colDefinitions.find((el) => el.name === name);
    if (!def) throw new Error('Missing col definition');

    return def;
  };

  protected abstract buildUpdateQuery(id: string, dto: UpdateDto): Query;

  updateOne = async (
    id: string,
    updateDto: UpdateDto,
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<string> => {
    try {
      const query = this.buildUpdateQuery(id, updateDto);

			if (!query.colDefinitions)
        throw new Error('No column definitions found. Cannot perform update operation');
			
			const document: any = {};
			query.colDefinitions.forEach((column, index) => {
				const value = query.values[index];
				document[column.name] = column.nullable && value === 'null' ? null : value;
			});

			const [docId, ...values] = Object.values(document);
      const fieldNames = Object.keys(document).slice(1);
      const newValues = Object.fromEntries(fieldNames.map((fieldName, index) => [fieldName, values[index]]));
			
      await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
			.updateOne(
				{ id: docId },
				{ $set: newValues }
			);

      return id;
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  replaceMany = async (
    entities: Entity[],
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<number> => {
    try {
      const rows = entities.map((column) => this.getValues(column));

      const documents = rows.map((bind) => {
				const document: any = {};
				this.colDefinitions.forEach((column, index) => {
					const value = bind[index];
					document[column.name] = column.nullable && value === 'null' ? null : value; 
				});
				return document;
			});

			await Promise.all(documents.map(async (doc) => {
				const [id, ...values] = Object.values(doc);
        const fieldNames = Object.keys(doc).slice(1);
        const newValues = Object.fromEntries(fieldNames.map((fieldName, index) => [fieldName, values[index]]));
				const res = await dbConnection
        .collection(`${this.matName}_${auth.callerOrgId}`)
				.updateOne(
					{ id },
					{ $set: newValues }
				);
        
				return res;
			}));

      return entities.length;
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  deleteAll = async (
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<void> => {
    try {
      const result = await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
      .deleteMany({});

      if (!result.acknowledged) throw new Error('Deletion was unsuccessful');
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
    }
  };

  deleteMany = async (
    ids: string[],
    auth: BaseAuth,
    dbConnection: IDbConnection
  ): Promise<number> => {
    try {
      const result = await dbConnection
      .collection(`${this.matName}_${auth.callerOrgId}`)
			.deleteMany({ id: { $in: ids } });

      if (result.deletedCount === 0) throw new Error('Deletion was unsuccessful');

      return ids.length;
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  protected static isOptionalOfType = <T>(
    val: unknown,
    targetType:
      | 'string'
      | 'number'
      | 'bigint'
      | 'boolean'
      | 'symbol'
      | 'undefined'
      | 'object'
      | 'function'
  ): val is T => val === null || typeof val === targetType;

  protected static isStringArray = (value: unknown): value is string[] =>
    Array.isArray(value) && value.every((el) => typeof el === 'string');

  protected abstract buildEntityProps(sfEntity: SnowflakeEntity): EntityProps;

  protected abstract toEntity(materializationProps: EntityProps): Entity;
}
