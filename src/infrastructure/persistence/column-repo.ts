import { Auth, ColumnQueryDto, IColumnRepo } from '../../domain/column/i-column-repo';
import { Column, ColumnProperties, parseColumnDataType } from '../../domain/entities/column';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import { ColumnDefinition, getInsertQuery } from './shared/query';

export default class ColunRepo implements IColumnRepo {
    readonly #matName = 'column';
  
    readonly #nonOptionalColDefinitions: ColumnDefinition[] = [
      { name: 'id' },
      { name: 'name' },
      { name: 'relation_name' },
      { name: 'index' },
      { name: 'data_type' },
      { name: 'materialization_id' },
      { name: 'lineage_ids', selectType: 'parse_json' },
    ];
      
    readonly #querySnowflake: QuerySnowflake;
  
    constructor(querySnowflake: QuerySnowflake) {
      this.#querySnowflake = querySnowflake;
    }

    #buildColumn = (sfEntity: SnowflakeEntity): Column => {const {
      ID: id,
      NAME: name,
      RELATION_NAME: relationName,
      INDEX: index,
      DATA_TYPE: dataType,
      IS_IDENTITY: isIdentity, IS_NULLABLE: isNullable, MATERIALIZATION_ID: materializationId, LINEAGE_IDS: lineageIds, COMMENT:comment
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof name !== 'string' ||
      typeof relationName !== 'string' ||
      typeof index !== 'string' ||
      typeof dataType !== 'string' ||
      typeof isIdentity !== 'boolean'||
      typeof isNullable !== 'boolean' ||
      typeof materializationId !== 'string' ||
      typeof lineageIds !== 'object' ||
      typeof comment !== 'string' 
    )
      throw new Error(
        'Retrieved unexpected column field types from persistence'
      );

      const isStringArray = (value: unknown): value is string[] => Array.isArray(value) && value.every(el => typeof el === 'string');

    if(!isStringArray(lineageIds)) throw new Error('Type mismatch detected when reading column from persistence');


    return this.#toEntity({
      id,
      name,
      relationName,
      index,
      dataType,
      isIdentity,
      isNullable,
      materializationId,
      lineageIds,
      comment
    });};

    findOne = async (
      columnId: string,
      targetOrgId: string,
      auth: Auth
    ): Promise<Column | null> => {
      try {
        const queryText = `select * from cito.lineage.${this.#matName}
          } where id = ?;`;
  
        // using binds to tell snowflake to escape params to avoid sql injection attack
        const binds: (string | number)[] = [columnId];
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
        if (result.value.length !== 1)
          throw new Error(`Multiple or no column entities with id found`);
  
        return !result.value.length ? null:  this.#buildColumn(result.value[0])
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error());
      }
    };
  
    findBy = async (
      columnQueryDto: ColumnQueryDto,
      targetOrgId: string,
      auth: Auth
    ): Promise<Column[]> => {
      try {
        if (!Object.keys(columnQueryDto).length)
          return await this.all(targetOrgId, auth);
  


        // using binds to tell snowflake to escape params to avoid sql injection attack
        const binds: (string | number)[] = [columnQueryDto.lineageId];
        let whereClause = 'array_contains(?::variant, lineage_ids) ';


        if (columnQueryDto.relationName) 
        {binds.push(Array.isArray(columnQueryDto.relationName) ? columnQueryDto.relationName.map(el => `'${el}'`).join(', '):  columnQueryDto.relationName);
        whereClause.concat( Array.isArray(columnQueryDto.relationName) ? 'and array_contains(relation_name::variant, array_construct(?))': 'and relation_name = ? ');
      }
        if (columnQueryDto.name) 
        {binds.push(Array.isArray(columnQueryDto.name) ? columnQueryDto.name.map(el => `'${el}'`).join(', '):  columnQueryDto.name);
        whereClause.concat( Array.isArray(columnQueryDto.name) ? 'and array_contains(name::variant, array_construct(?))': 'and name = ? ');
      
      }
        if (columnQueryDto.index) 
        {binds.push(columnQueryDto.index);
      whereClause.concat('and index = ? ')  
        }
        if (columnQueryDto.type) 
        {binds.push(columnQueryDto.type);
      whereClause.concat('and type = ? ')  
        
        }
        if (columnQueryDto.materializationId) 
        {binds.push(Array.isArray(columnQueryDto.materializationId) ? columnQueryDto.materializationId.map(el => `'${el}'`).join(', '):  columnQueryDto.materializationId);
        whereClause.concat( Array.isArray(columnQueryDto.materializationId) ? 'and array_contains(materializationId::variant, array_construct(?))': 'and materializationId = ? ');
      
      }
  
        const queryText = `select * from cito.lineage.${this.#matName}
        } where  ${whereClause};`;
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
        if (result.value.length !== 1)
          throw new Error(`Multiple or no column entities with id found`);
  
        return result.value.map((el) => this.#buildColumn(el));
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error());
      }
    };
  
    all = async (targetOrgId: string, auth: Auth): Promise<Column[]> => {
      try {
        const queryText = `select * from cito.lineage.${this.#matName};`;
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds: [] },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
        if (result.value.length !== 1)
          throw new Error(`Multiple or no column entities with id found`);
  
        return result.value.map((el) => this.#buildColumn(el));
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error());
      }
    };
  

    #getInsertQueryProps = (column: Column) : {colDefinitions: ColumnDefinition[], binds: (string|number)[]|(string|number)[][], rows: string[]} => {
      const binds = [
        column.id,
        column.name,
        column.relationName,
        column.index,
        column.dataType,
        column.materializationId,
        JSON.stringify(column.lineageIds),
      ];
      
      const localColDefinitions = this.#nonOptionalColDefinitions;
      
      if(column.isIdentity !== undefined)
      {binds.push(column.isIdentity.toString());
      
      localColDefinitions.push({ name: 'is_identity' },)}
      if(column.isNullable !== undefined)
      {binds.push(column.isNullable.toString(),)
      localColDefinitions.push({ name: 'is_nullable' })
      }
      if(column.comment)
      {binds.push(column.comment); localColDefinitions.push({name: 'comment'})}

      const row = `(${binds.map(() => '?').join(', ')})`;


      return {colDefinitions: localColDefinitions, binds, rows: [row]};
    }

    insertOne = async (
      column: Column,
      targetOrgId: string,
      auth: Auth
    ): Promise<string> => {
      try {
        const queryProps = this.#getInsertQueryProps(column);

        const queryText = getInsertQuery(this.#matName, queryProps.colDefinitions, queryProps.rows);
  
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds: queryProps.binds },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
  
        return column.id;
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error());
      }
    };
  
    insertMany = async (
      columns: Column[],
      targetOrgId: string,
      auth: Auth
    ): Promise<string[]> => {
      try {
        const queryText = getInsertQuery(this.#matName, queryProps.colDefinitions, queryProps.rows);

        todo - how to handle col definitions that do not include

        const queryPropsElements = columns.map((el) => this.#getInsertQueryProps(el));

        const 
  
        const rows = binds.map((el) => `(${el.map(() => '?').join(', ')})`);
  
        const queryText = getInsertQuery(
          this.#matName,
          this.#colDefinitions,
          rows
        );
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
  
        return columns.map((el) => el.id);
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error());
      }
    };
  
    replaceMany = async (
      columns: Column[],
      targetOrgId: string,
      auth: Auth
    ): Promise<number> => {
      try {
        const binds = columns.map((el) => [
          el.id,
          el.relationName,
          el.sql,
          JSON.stringify(el.dependentOn),
          el.parsedColumn,
          JSON.stringify(el.statementRefs),
          JSON.stringify(el.lineageIds),
        ]);
  
        const rows = binds.map((el) => `(${el.map(() => '?').join(', ')})`);
  
        const queryText = getUpdateQuery(this.#matName, this.#colDefinitions, rows,
        );
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
  
        return columns.length;
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error());
      }
    };
  
    #toEntity = (columnProperties: ColumnProps): Column =>
      Column.build(columnProperties);
  }



  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  findOne = async (id: string, dbConnection: Db): Promise<Column | null> => {
    try {
      const {
        ID: id,
        NAME: name,
        RELATION_NAME: relationName,
        INDEX: index,
        DATA_TYPE: dataType,
        IS_IDENTITY: isIdentity,
        IS_NULLABLE: isNullable,
        MATERIALIZATION_ID: materializationId,
        LINEAGE_IDS: lineageIds,
        COMMENT: comment,
      } = result[0];

      if (
        typeof id !== 'string' ||
        typeof name !== 'string' ||
        typeof relationName !== 'string' ||
        typeof index !== 'string' ||
        typeof dataType !== 'string' ||
        typeof isIdentity !== 'boolean' ||
        typeof isNullable !== 'boolean' ||
        typeof materializationId !== 'string' ||
        typeof lineageIds !== 'string' ||
        typeof comment !== 'string'
      )
        throw new Error(
          'Retrieved unexpected column field types from persistence'
        );


      const result: any = await dbConnection
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findBy = async (
    columnQueryDto: ColumnQueryDto,
    dbConnection: Db
  ): Promise<Column[]> => {
    try {
      if (!Object.keys(columnQueryDto).length)
        return await this.all(dbConnection);

      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(columnQueryDto)));
      const results = await result.toArray();

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #buildFilter = (columnQueryDto: ColumnQueryDto): ColumnQueryFilter => {
    const filter: ColumnQueryFilter = {
      lineageIds: columnQueryDto.lineageId,
      organizationId: columnQueryDto.organizationId,
    };

    if (
      typeof columnQueryDto.relationName === 'string' &&
      columnQueryDto.relationName
    )
      filter.relationName = new RegExp(`^${columnQueryDto.relationName}$`, 'i');
    if (columnQueryDto.relationName instanceof Array)
      filter.relationName = {
        $in: columnQueryDto.relationName.map(
          (element) => new RegExp(`^${element}$`, 'i')
        ),
      };

    if (typeof columnQueryDto.name === 'string' && columnQueryDto.name)
      filter.name = new RegExp(`^${columnQueryDto.name}$`, 'i');
    if (columnQueryDto.name instanceof Array)
      filter.name = {
        $in: columnQueryDto.name.map(
          (element) => new RegExp(`^${element}$`, 'i')
        ),
      };

    if (columnQueryDto.index) filter.index = columnQueryDto.index;
    if (columnQueryDto.type) filter.type = columnQueryDto.type;

    if (
      typeof columnQueryDto.materializationId === 'string' &&
      columnQueryDto.materializationId
    )
      filter.materializationId = columnQueryDto.materializationId;
    if (columnQueryDto.materializationId instanceof Array)
      filter.materializationId = { $in: columnQueryDto.materializationId };

    return filter;
  };

  all = async (dbConnection: Db): Promise<Column[]> => {
    try {
      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find();
      const results = await result.toArray();

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertOne = async (column: Column, dbConnection: Db): Promise<string> => {
    try {
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(column)));

      if (!result.acknowledged)
        throw new Error('Column creation failed. Insert not acknowledged');

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertMany = async (
    columns: Column[],
    dbConnection: Db
  ): Promise<string[]> => {
    try {
      const result: InsertManyResult<Document> = await dbConnection
        .collection(collectionName)
        .insertMany(
          columns.map((element) => this.#toPersistence(sanitize(element)))
        );

      if (!result.acknowledged)
        throw new Error('Column creations failed. Inserts not acknowledged');

      return Object.keys(result.insertedIds).map((key) =>
        result.insertedIds[parseInt(key, 10)].toHexString()
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  replaceMany = async (
    columns: Column[],
    dbConnection: Db
  ): Promise<number> => {
    try {
      const operations: AnyBulkWriteOperation<Document>[] = columns.map(
        (el) => ({
          replaceOne: {
            filter: { _id: new ObjectId(sanitize(el.id)) },
            replacement: this.#toPersistence(el),
          },
        })
      );

      const result: BulkWriteResult = await dbConnection
        .collection(collectionName)
        .bulkWrite(operations);

      if (!result.isOk())
        throw new Error(
          `Bulk mat update failed. Update not ok. Error count: ${result.getWriteErrorCount()}`
        );

      return result.nMatched;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };



  #toEntity = (properties: ColumnProperties): Column =>
    Column.build(properties);

  #buildProperties = (column: ColumnPersistence): ColumnProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: column._id.toHexString(),
    relationName: column.relationName,
    name: column.name,
    index: column.index,
    dataType: parseColumnDataType(column.dataType),
    materializationId: column.materializationId,
    lineageIds: column.lineageIds,
    organizationId: column.organizationId,
  });

  #toPersistence = (column: Column): Document => ({
    _id: ObjectId.createFromHexString(column.id),
    relationName: column.relationName,
    name: column.name,
    index: column.index,
    type: column.dataType,
    materializationId: column.materializationId,
    lineageIds: column.lineageIds,
    organizationId: column.organizationId,
  });
}
