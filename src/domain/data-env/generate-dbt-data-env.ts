import { CreateColumn } from '../column/create-column';
import { Column, ColumnDataType } from '../entities/column';
import {
  Logic,
  MaterializationDefinition,
  ModelRepresentation,
  Refs,
} from '../entities/logic';
import {
  Materialization,
  MaterializationType,
  materializationTypes,
  parseMaterializationType,
} from '../entities/materialization';
import { CreateLogic } from '../logic/create-logic';
import {
  CreateMaterialization,
  CreateMaterializationRequestDto,
} from '../materialization/create-materialization';
import BaseAuth from '../services/base-auth';
import IUseCase from '../services/use-case';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import Result from '../value-types/transient-types/result';
import { DataEnvProps } from './data-env';

export interface GenerateDbtDataEnvRequestDto {
  dbtCatalog: string;
  dbtManifest: string;
  targetOrgId?: string;
}

export type GenerateDbtDataEnvAuthDto = BaseAuth;

export type GenerateDbtDataEnvResponse = Result<DataEnvProps>;

interface DbtNodeMetadata {
  name: string;
  schema: string;
  database: string;
  type: string;
}

interface DbtCatalogNode {
  metadata: DbtNodeMetadata;
  columns: { [key: string]: DbtCatalogColumnDefinition };
  [key: string]: unknown;
}

interface DbtManifestNode {
  relation_name: string;
  compiled_sql: string;
  [key: string]: unknown;
}

type DbtCatalogSource = {
  metadata: DbtNodeMetadata;
  columns: {
    [key: string]: DbtCatalogColumnDefinition;
  };
  [key: string]: unknown;
};

type DbtManifestSource = {
  relation_name: string;
  [key: string]: unknown;
};

interface DbtCatalogResources {
  nodes: { [key: string]: DbtCatalogNode };
  sources: { [key: string]: DbtCatalogSource };
}

interface DbtCatalog extends DbtCatalogResources {
  [key: string]: unknown;
}

interface DbtManifest extends Omit<DbtManifestResources, 'parentMap'> {
  [key: string]: unknown;
  parent_map: { [key: string]: string[] };
}

interface DbtManifestResources {
  nodes: { [key: string]: DbtManifestNode };
  sources: { [key: string]: DbtManifestSource };
  parentMap: { [key: string]: string[] };
}

interface DbtCatalogColumnDefinition {
  name: string;
  index: string;
  dataType: ColumnDataType;
}

export class GenerateDbtDataEnv
  implements
    IUseCase<
      GenerateDbtDataEnvRequestDto,
      GenerateDbtDataEnvResponse,
      GenerateDbtDataEnvAuthDto,
      IConnectionPool
    >
{
  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #parseSQL: ParseSQL;

  readonly #createLogic: CreateLogic;

  readonly #materializations: Materialization[] = [];

  get materializations(): Materialization[] {
    return this.#materializations;
  }

  #columns: Column[] = [];

  get columns(): Column[] {
    return this.#columns;
  }

  #logics: Logic[] = [];

  get logics(): Logic[] {
    return this.#logics;
  }

  #catalog: ModelRepresentation[] = [];

  #auth?: GenerateDbtDataEnvAuthDto;

  #req?: GenerateDbtDataEnvRequestDto;

  #connPool?: IConnectionPool;

  constructor(
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createLogic: CreateLogic,
    parseSQL: ParseSQL
  ) {
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createLogic = createLogic;
    this.#parseSQL = parseSQL;
  }

  /* Get dbt nodes from catalog.json */
  #getDbtCatalogResources = (): DbtCatalogResources => {
    if (!this.#req)
      throw new Error(
        'Missing props detected while generating dbt based data env'
      );

    const dbtCatalog: DbtCatalog = JSON.parse(this.#req.dbtCatalog);
    const { nodes, sources } = dbtCatalog;

    return { nodes, sources };
  };

  /* Get dbt nodes from manifest.json */
  #getDbtManifestResources = (): DbtManifestResources => {
    if (!this.#req)
      throw new Error(
        'Missing props detected while generating dbt based data env'
      );

    const dbtManifest: DbtManifest = JSON.parse(this.#req.dbtManifest);
    const { nodes, sources, parent_map: parentMap } = dbtManifest;

    return { nodes, sources, parentMap };
  };

  #generateColumn = async (
    columnDefinition: DbtCatalogColumnDefinition,
    sourceRelationName: string,
    sourceId: string
  ): Promise<Column> => {
    if (!this.#connPool || !this.#req || !this.#auth)
      throw new Error('Generating dbt based data env - Props missing');

    const createColumnResult = await this.#createColumn.execute(
      {
        relationName: sourceRelationName,
        name: columnDefinition.name,
        index: columnDefinition.index,
        dataType: columnDefinition.dataType,
        materializationId: sourceId,
        writeToPersistence: false,
        targetOrgId: this.#req.targetOrgId,
      },
      this.#auth,
      this.#connPool
    );

    if (!createColumnResult.success) throw new Error(createColumnResult.error);
    if (!createColumnResult.value)
      throw new SyntaxError(`Creation of column failed`);

    return createColumnResult.value;
  };

  /* Runs through manifest source objects and creates corresponding materialization objecst */
  #generateWarehouseSource = async (
    sourceProps: {
      type: MaterializationType;
      name: string;
      relationName: string;
      schemaName: string;
      databaseName: string;
      logicId?: string;
      columns: {
        [key: string]: DbtCatalogColumnDefinition;
      };
    },
    options: { writeToPersistence: boolean }
  ): Promise<void> => {
    if (!this.#connPool || !this.#req || !this.#auth)
      throw new Error('Generating dbt based data env - Props missing');

    const { columns, ...createMaterializationProps } = sourceProps;

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        {
          ...createMaterializationProps,

          writeToPersistence: options.writeToPersistence,
          targetOrgId: this.#req.targetOrgId,
        },
        this.#auth,
        this.#connPool
      );

    if (!createMaterializationResult.success)
      throw new Error(createMaterializationResult.error);
    if (!createMaterializationResult.value)
      throw new SyntaxError(`Creation of materialization failed`);

    const materialization = createMaterializationResult.value;

    const generatedColumns = await Promise.all(
      Object.keys(columns).map(async (columnKey) =>
        this.#generateColumn(
          columns[columnKey],
          materialization.relationName,
          materialization.id
        )
      )
    );

    this.#materializations.push(materialization);
    this.#columns.push(...generatedColumns);
  };

  /* Sends sql to parse SQL microservices and receives parsed SQL logic back */
  #parseLogic = async (sql: string): Promise<string> => {
    const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute({
      dialect: 'snowflake',
      sql,
    });

    if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
    if (!parseSQLResult.value)
      throw new SyntaxError(`Parsing of SQL logic failed`);

    return JSON.stringify(parseSQLResult.value);
  };

  /* Create materializations and columns that are referenced in SQL models, but are neither defined as node or source objects in manifest.json */
  #createExternalResources = async (
    statementRefs: Refs,
    dwMatDefinitions: MaterializationDefinition[]
  ): Promise<void> => {
    await Promise.all(
      dwMatDefinitions.map(async (def) => {
        const matchingMaterializations = statementRefs.materializations.filter(
          (el) =>
            (typeof def.databaseName === 'string' &&
            typeof el.databaseName === 'string'
              ? el.databaseName === def.databaseName
              : def.databaseName === el.databaseName) &&
            (typeof def.schemaName === 'string' &&
            typeof el.schemaName === 'string'
              ? el.schemaName === def.schemaName
              : def.schemaName === el.schemaName) &&
            el.name === def.materializationName
        );

        if (matchingMaterializations.length === 0) return;
        if (matchingMaterializations.length > 1)
          throw new Error(
            'Multiple matching materialization refs in logic found'
          );

        const matRef = matchingMaterializations[0];

        const existingMat = this.#materializations.find(
          (el) => el.relationName === def.relationName
        );

        let mat = existingMat;
        if (!existingMat) {
          if (!this.#connPool || !this.#req || !this.#auth)
            throw new Error('Generating dbt based data env - Props missing');

          const createMaterializationResult =
            await this.#createMaterialization.execute(
              {
                type: 'base table',
                name: matRef.name,
                relationName: def.relationName,
                schemaName: matRef.schemaName || '',
                databaseName: matRef.databaseName || '',
                // 'todo - read from snowflake'
                logicId: undefined,
                targetOrgId: this.#req.targetOrgId,
                writeToPersistence: false,
              },
              this.#auth,
              this.#connPool
            );

          if (!createMaterializationResult.success)
            throw new Error(createMaterializationResult.error);
          if (!createMaterializationResult.value)
            throw new SyntaxError(`Creation of materialization failed`);

          mat = createMaterializationResult.value;

          this.#materializations.push(mat);
        }

        const finalMat = mat;
        if (!finalMat)
          throw new Error(
            'Creating external resources -> Materialization obj not found'
          );

        const relevantColumnRefs = statementRefs.columns.filter(
          (col) => finalMat.name === col.materializationName
        );

        const uniqueRelevantColumnRefs = relevantColumnRefs.filter(
          (column1, index, self) =>
            index === self.findIndex((column2) => column1.name === column2.name)
        );

        const isColumn = (column: Column | undefined): column is Column =>
          !!column;

        const columns = (
          await Promise.all(
            uniqueRelevantColumnRefs.map(async (el) =>
              this.#generateColumn(
                { name: el.name, index: '-1', dataType: 'string' },
                finalMat.relationName,
                finalMat.id
              )
            )
          )
        ).filter(isColumn);

        this.#columns.push(...columns);

        // wildcards;
      })
    );
  };

  #generateNodeMaterialization = async (
    req: CreateMaterializationRequestDto
  ): Promise<Materialization> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Generating dbt based data env - Props missing');

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        req,
        this.#auth,
        this.#connPool
      );

    if (!createMaterializationResult.success)
      throw new Error(createMaterializationResult.error);
    if (!createMaterializationResult.value)
      throw new SyntaxError(`Creation of materialization failed`);

    const materialization = createMaterializationResult.value;

    this.#materializations.push(materialization);

    return materialization;
  };

  /* Generate dbt model node */
  #generateDbtModelNode = async (props: {
    model: DbtCatalogNode;
    modelManifest: DbtManifestNode;
    dbtDependentOn: MaterializationDefinition[];
  }): Promise<void> => {
    if (!this.#connPool || !this.#req || !this.#auth)
      throw new Error('Generating dbt based data env - Props missing');

    const sql = props.modelManifest.compiled_sql;

    const parsedLogic = await this.#parseLogic(sql);

    const createLogicResult = await this.#createLogic.execute(
      {
        props: {
          generalProps: {
            relationName: props.modelManifest.relation_name,
            sql,
            parsedLogic,
            targetOrgId: this.#req.targetOrgId,
            catalog: this.#catalog,
          },
          dbtProps: {
            dbtDependentOn: props.dbtDependentOn,
          },
        },
        options: {
          writeToPersistence: false,
        },
      },
      this.#auth,
      this.#connPool
    );

    if (!createLogicResult.success) throw new Error(createLogicResult.error);
    if (!createLogicResult.value)
      throw new SyntaxError(`Creation of logic failed`);

    const logic = createLogicResult.value;

    this.#logics.push(logic);

    await this.#createExternalResources(
      logic.statementRefs,
      logic.dependentOn.dwDependencyDefinitions
    );

    const mat = await this.#generateNodeMaterialization({
      type: parseMaterializationType(props.model.metadata.type),
      name: props.model.metadata.name,
      relationName: props.modelManifest.relation_name,
      schemaName: props.model.metadata.schema,
      databaseName: props.model.metadata.database,
      logicId: logic.id,
      targetOrgId: this.#req.targetOrgId,
      writeToPersistence: false,
    });

    const columns = await Promise.all(
      Object.keys(props.model.columns).map(async (columnKey) =>
        this.#generateColumn(
          props.model.columns[columnKey],
          mat.relationName,
          mat.id
        )
      )
    );

    this.#columns.push(...columns);
  };

  /* Genereates dbt node of type node */
  #generateDbtSeedNode = async (props: {
    model: DbtCatalogNode;
    modelManifest: DbtManifestNode;
    dbtDependentOn: MaterializationDefinition[];
  }): Promise<void> => {
    if (!this.#req)
      throw new Error('Generating dbt based data env - Props missing');

    const mat = await this.#generateNodeMaterialization({
      type: parseMaterializationType(props.model.metadata.type),
      name: props.model.metadata.name,
      relationName: props.modelManifest.relation_name,
      schemaName: props.model.metadata.schema,
      databaseName: props.model.metadata.database,
      targetOrgId: this.#req.targetOrgId,
      writeToPersistence: false,
    });

    const columns = await Promise.all(
      Object.keys(props.model.columns).map(async (columnKey) =>
        this.#generateColumn(
          props.model.columns[columnKey],
          mat.relationName,
          mat.id
        )
      )
    );

    this.#columns.push(...columns);
  };

  /* Generate resources that are either defined in catalog and manifest.json 
  or are only reference by SQL models (e.g. sf source table that are not defined as dbt sources) */
  #generateDbtNode = async (props: {
    model: DbtCatalogNode;
    modelManifest: DbtManifestNode;
    dbtDependentOn: MaterializationDefinition[];
  }): Promise<void> => {
    switch (props.modelManifest.resource_type) {
      case 'model':
        await this.#generateDbtModelNode({ ...props });
        break;
      case 'seed':
        await this.#generateDbtSeedNode({ ...props });
        break;
      case 'test':
        break;
      default:
        throw new Error(
          'Unhandled dbt node type detected while generating dbt node resources'
        );
    }
  };

  /* Runs through snowflake and creates objects like logic, materializations and columns */
  async execute(
    req: GenerateDbtDataEnvRequestDto,
    auth: GenerateDbtDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<GenerateDbtDataEnvResponse> {
    try {
      this.#connPool = connPool;
      this.#req = req;
      this.#auth = auth;

      const uniqueIdRelationNameMapping: {
        [key: string]: { relationName: string };
      } = {};

      const dbtCatalogResources = this.#getDbtCatalogResources();
      const dbtManifestResources = this.#getDbtManifestResources();

      const dbtCatalogSourceKeys = Object.keys(dbtCatalogResources.sources);
      const dbtManifestSourceKeys = Object.keys(dbtManifestResources.sources);

      dbtManifestSourceKeys.forEach((key: string) => {
        uniqueIdRelationNameMapping[key] = {
          relationName: dbtManifestResources.sources[key].relation_name,
        };
      });

      dbtCatalogSourceKeys.forEach((key) => {
        const source = dbtCatalogResources.sources[key];

        const modelRepresentation: ModelRepresentation = {
          relationName: uniqueIdRelationNameMapping[key].relationName,
          materializationName: source.metadata.name,
          schemaName: source.metadata.schema,
          databaseName: source.metadata.database,
          columnNames: Object.keys(source.columns).map(
            (colKey) => source.columns[colKey].name
          ),
        };

        this.#catalog.push(modelRepresentation);
      });

      await Promise.all(
        dbtCatalogSourceKeys.map(async (key) => {
          const source = dbtCatalogResources.sources[key];

          const { name, type, schema, database } = source.metadata;

          if (
            (typeof type !== 'string' && type in materializationTypes) ||
            typeof name !== 'string' ||
            typeof schema !== 'string' ||
            typeof database !== 'string'
          )
            throw new TypeError(
              "Unexpected type coming from one of manifest or catalog.json's fields"
            );

          const sourceProps = {
            type: parseMaterializationType(type),
            name,
            relationName: uniqueIdRelationNameMapping[key].relationName,
            schemaName: schema,
            databaseName: database,
            // 'todo - read from snowflake'
            logicId: undefined,
            columns: source.columns,
          };

          const options = {
            writeToPersistence: false,
          };

          await this.#generateWarehouseSource(sourceProps, options);
        })
      );

      const dbtCatalogNodeKeys = Object.keys(dbtCatalogResources.nodes);

      if (!dbtCatalogNodeKeys.length)
        throw new ReferenceError('No dbt models found');

      const dbtManifestNodeKeys = Object.keys(dbtManifestResources.nodes);

      dbtManifestNodeKeys.forEach((key: string) => {
        uniqueIdRelationNameMapping[key] = {
          relationName: dbtManifestResources.nodes[key].relation_name,
        };
      });

      dbtCatalogNodeKeys.forEach((key) => {
        const model = dbtCatalogResources.nodes[key];

        const modelRepresentation: ModelRepresentation = {
          relationName: uniqueIdRelationNameMapping[key].relationName,
          materializationName: model.metadata.name,
          schemaName: model.metadata.schema,
          databaseName: model.metadata.database,
          columnNames: Object.keys(model.columns).map(
            (colKey) => model.columns[colKey].name
          ),
        };

        this.#catalog.push(modelRepresentation);
      });

      await Promise.all(
        dbtCatalogNodeKeys.map(async (key) => {
          const dependsOn: string[] = [
            ...new Set(dbtManifestResources.parentMap[key]),
          ];

          const dependsOnRelationName = dependsOn.map(
            (dependencyKey) =>
              uniqueIdRelationNameMapping[dependencyKey].relationName
          );

          const dbtDependentOn = this.#catalog.filter((el) =>
            dependsOnRelationName.includes(el.relationName)
          );

          if (dependsOnRelationName.length !== dbtDependentOn.length)
            throw new RangeError('materialization dependency mismatch');

          return this.#generateDbtNode({
            model: dbtCatalogResources.nodes[key],
            modelManifest: dbtManifestResources.nodes[key],
            dbtDependentOn,
          });
        })
      );

      return Result.ok({
        dataEnv: {
          matsToCreate: this.#materializations,
          columnsToCreate: this.#columns,
          logicsToCreate: this.#logics,
          columnsToReplace: [],
          columnToDeleteRefs: [],
          logicsToReplace: [],
          logicToDeleteRefs: [],
          matsToReplace: [],
          matToDeleteRefs: [],
        },
        catalog: this.#catalog,
        // todo - needs to be updated. Not retrieved
        dbCoveredNames: [],
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
