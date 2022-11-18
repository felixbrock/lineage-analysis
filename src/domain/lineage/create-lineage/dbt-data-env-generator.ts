import { CreateColumn } from '../../column/create-column';
import { Column, ColumnDataType } from '../../entities/column';
import {
  Logic,
  MaterializationDefinition,
  ModelRepresentation,
  Refs,
} from '../../entities/logic';
import {
  Materialization,
  MaterializationType,
  materializationTypes,
  parseMaterializationType,
} from '../../entities/materialization';
import { CreateLogic } from '../../logic/create-logic';
import {
  CreateMaterialization,
  CreateMaterializationRequestDto,
} from '../../materialization/create-materialization';
import BaseAuth from '../../services/base-auth';

import { IConnectionPool } from '../../snowflake-api/i-snowflake-api-repo';
import { ParseSQL, ParseSQLResponseDto } from '../../sql-parser-api/parse-sql';
import { GenerateResult, IDataEnvGenerator } from './i-data-env-generator';

export type Auth = BaseAuth;

export interface DbtDataEnvProps {
  lineageId: string;
  dbtCatalog: string;
  dbtManifest: string;
  targetOrgId?: string;
}

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

export class DbtDataEnvGenerator implements IDataEnvGenerator {
  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #parseSQL: ParseSQL;

  readonly #createLogic: CreateLogic;

  readonly #lineageId: string;

  readonly #dbtCatalog: DbtCatalog;

  readonly #dbtManifest: DbtManifest;

  readonly #targetOrgId?: string;

  readonly #auth: Auth;

  readonly #materializations: Materialization[] = [];

  #connPool?: IConnectionPool;

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

  constructor(
    props: DbtDataEnvProps,
    auth: Auth,
    dependencies: {
      createMaterialization: CreateMaterialization;
      createColumn: CreateColumn;
      parseSQL: ParseSQL;
      createLogic: CreateLogic;
    }
  ) {
    this.#createMaterialization = dependencies.createMaterialization;
    this.#createColumn = dependencies.createColumn;
    this.#parseSQL = dependencies.parseSQL;
    this.#createLogic = dependencies.createLogic;

    this.#auth = auth;

    this.#lineageId = props.lineageId;
    this.#dbtCatalog = JSON.parse(props.dbtCatalog);
    this.#dbtManifest = JSON.parse(props.dbtManifest);
    this.#targetOrgId = props.targetOrgId;
  }

  /* Get dbt nodes from catalog.json */
  #getDbtCatalogResources = (): DbtCatalogResources => {
    const { nodes, sources } = this.#dbtCatalog;

    return { nodes, sources };
  };

  /* Get dbt nodes from manifest.json */
  #getDbtManifestResources = (): DbtManifestResources => {
    const { nodes, sources, parent_map: parentMap } = this.#dbtManifest;

    return { nodes, sources, parentMap };
  };

  #generateColumn = async (
    columnDefinition: DbtCatalogColumnDefinition,
    sourceRelationName: string,
    sourceId: string
  ): Promise<Column> => {
    if (!this.#connPool) throw new Error('Connection pool missing');

    const createColumnResult = await this.#createColumn.execute(
      {
        relationName: sourceRelationName,
        name: columnDefinition.name,
        index: columnDefinition.index,
        dataType: columnDefinition.dataType,
        materializationId: sourceId,
        lineageId: this.#lineageId,
        writeToPersistence: false,
        targetOrgId: this.#targetOrgId,
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
    if (!this.#connPool) throw new Error('Connection pool missing');

    const { columns, ...createMaterializationProps } = sourceProps;

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        {
          ...createMaterializationProps,

          writeToPersistence: options.writeToPersistence,
          lineageId: this.#lineageId,
          targetOrgId: this.#targetOrgId,
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
    const connPool = this.#connPool;
    if (!connPool) throw new Error('Connection pool missing');

    await Promise.all(
      dwMatDefinitions.map(async (def) => {
        const matchingMaterializations = statementRefs.materializations.filter(
          (el) =>
            (typeof def.databaseName === 'string' &&
            typeof el.databaseName === 'string'
              ? DbtDataEnvGenerator.#insensitiveEquality(
                  el.databaseName,
                  def.databaseName
                )
              : def.databaseName === el.databaseName) &&
            (typeof def.schemaName === 'string' &&
            typeof el.schemaName === 'string'
              ? DbtDataEnvGenerator.#insensitiveEquality(
                  el.schemaName,
                  def.schemaName
                )
              : def.schemaName === el.schemaName) &&
            DbtDataEnvGenerator.#insensitiveEquality(
              el.name,
              def.materializationName
            )
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
                lineageId: this.#lineageId,
                targetOrgId: this.#targetOrgId,
                writeToPersistence: false,
              },
              this.#auth,
              connPool
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

        const relevantColumnRefs = statementRefs.columns.filter((col) =>
          DbtDataEnvGenerator.#insensitiveEquality(
            finalMat.name,
            col.materializationName
          )
        );

        const uniqueRelevantColumnRefs = relevantColumnRefs.filter(
          (column1, index, self) =>
            index ===
            self.findIndex((column2) =>
              DbtDataEnvGenerator.#insensitiveEquality(
                column1.name,
                column2.name
              )
            )
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
    if (!this.#connPool) throw new Error('Connection pool missing');

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
    if (!this.#connPool) throw new Error('Connection pool missing');

    const sql = props.modelManifest.compiled_sql;

    const parsedLogic = await this.#parseLogic(sql);

    const createLogicResult = await this.#createLogic.execute(
      {
        props: {
          generalProps: {
            relationName: props.modelManifest.relation_name,
            sql,
            lineageId: this.#lineageId,
            parsedLogic,
            targetOrgId: this.#targetOrgId,
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
      lineageId: this.#lineageId,
      targetOrgId: this.#targetOrgId,
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
    const mat = await this.#generateNodeMaterialization({
      type: parseMaterializationType(props.model.metadata.type),
      name: props.model.metadata.name,
      relationName: props.modelManifest.relation_name,
      schemaName: props.model.metadata.schema,
      databaseName: props.model.metadata.database,
      lineageId: this.#lineageId,
      targetOrgId: this.#targetOrgId,
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

  /* Runs through dbt nodes and creates objects like logic, materializations and columns */
  generate = async (connPool: IConnectionPool): Promise<GenerateResult> => {
    this.#connPool = connPool;

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

    return {
      catalog: this.#catalog,
      materializations: this.#materializations,
      columns: this.#columns,
      logics: this.#logics,
    };
  };

  static #insensitiveEquality = (str1: string, str2: string): boolean =>
    str1.toLowerCase() === str2.toLowerCase();
}
