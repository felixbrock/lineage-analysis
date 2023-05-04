// todo clean architecture violation
import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { EnvLineage } from './env-lineage';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import { Auth } from '../external-data-env/base-get-sf-external-data-env';
import { Dependency } from '../entities/dependency';
import { IDb, IDbConnection } from '../services/i-db';
import GenerateSfEnvLineageRepo from '../../infrastructure/persistence/generate-sf-env-lineage-repo';

export type GenerateSfEnvLineageRequestDto = undefined;

export interface GenerateSfEnvLineageAuthDto
  extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export type GenerateSfEnvLineageResponse = Result<EnvLineage>;

interface SfObjectRef {
  id: number;
  dbName: string;
  schemaName: string;
  matName: string;
  type: string;
}

export interface DatabaseRepresentation {
  name: string;
  ownerId: string;
  isTransient: boolean;
  comment?: string;
}

export const sfObjDependencyTypes = [
  'BY_NAME',
  'BY_ID',
  'BY_NAME_AND_ID',
] as const;
export type SfObjDependencyType = typeof sfObjDependencyTypes[number];

export const parseSfObjDependencyType = (
  type: unknown
): SfObjDependencyType => {
  if (typeof type !== 'string')
    throw new Error('Provision of type in non-string format');

  const identifiedElement = sfObjDependencyTypes.find(
    (element) => element.toLowerCase() === type.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

interface SfObjectDependency {
  head: SfObjectRef;
  tail: SfObjectRef;
  type: SfObjDependencyType;
}

export interface CitoMatRepresentation {
  id: string;
  relationName: string;
}
export class GenerateSfEnvLineage
  implements
    IUseCase<
      GenerateSfEnvLineageRequestDto,
      GenerateSfEnvLineageResponse,
      GenerateSfEnvLineageAuthDto,
      IDb
    >
{
  readonly querySnowflake: QuerySnowflake;

  #generateRepo: GenerateSfEnvLineageRepo;

  #connPool?: IConnectionPool;

  #auth?: Auth;

  #dbConnection?: IDbConnection;

  constructor(querySnowflake: QuerySnowflake, generateSfEnvLineageRepo: GenerateSfEnvLineageRepo) {
    this.querySnowflake = querySnowflake;
    this.#generateRepo = generateSfEnvLineageRepo;
  }

  #getDistinctRelationNames = async (
    sfObjDependencies: SfObjectDependency[]
  ): Promise<string[]> => {
    const distinctRelationNames = sfObjDependencies.reduce(
      (accumulation: string[], val: SfObjectDependency) => {
        const localAcc = accumulation;

        const refNameHead = `${val.head.dbName}.${val.head.schemaName}.${val.head.matName}`;
        const refNameTail = `${val.tail.dbName}.${val.tail.schemaName}.${val.tail.matName}`;

        if (!localAcc.includes(refNameHead)) localAcc.push(refNameHead);

        if (!localAcc.includes(refNameTail)) localAcc.push(refNameTail);

        return localAcc;
      },
      []
    );

    return distinctRelationNames;
  };

  #getAllLineageMats = async (
    relationNames: string[]
  ): Promise<CitoMatRepresentation[]> => {
    if (!this.#dbConnection || !this.#auth || !this.#auth.callerOrgId)
      throw new Error('Missing properties for generating sf data env');

    const results = await this.#generateRepo.findBy(relationNames, this.#dbConnection, this.#auth.callerOrgId);

    if (!results) return [];

    const citoMatRepresentations = results.map((el): CitoMatRepresentation => {
      const { id, relation_name: relationName } = el;

      if (typeof id !== 'string' || typeof relationName !== 'string')
        throw new Error('Received cito mat in unexpected format');

      return { id, relationName };
    });

    return citoMatRepresentations;
  };

  #getSfObjectDependencies = async (): Promise<SfObjectDependency[]> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Missing properties for generating sf data env');

    const dbRepresentations = await this.#getDbRepresentations();
    const dbNames = dbRepresentations.map((el) => `'${el.name}'`).join(', ');

    const queryText = `select * from snowflake.account_usage.object_dependencies 
      where (REFERENCED_OBJECT_DOMAIN = 'TABLE' or REFERENCED_OBJECT_DOMAIN = 'VIEW') 
        and (REFERENCING_OBJECT_DOMAIN = 'TABLE' or REFERENCING_OBJECT_DOMAIN = 'VIEW')
        and array_contains(REFERENCED_DATABASE::variant, array_construct(${dbNames}))
        and array_contains(REFERENCING_DATABASE::variant, array_construct(${dbNames}));`;

    const queryResult = await this.querySnowflake.execute(
      { queryText, binds: [] },
      this.#auth,
      this.#connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const dependencies: SfObjectDependency[] = results.map((el) => {
      const {
        REFERENCED_DATABASE: tailDbName,
        REFERENCED_SCHEMA: tailSchemaName,
        REFERENCED_OBJECT_NAME: tailMatName,
        REFERENCED_OBJECT_ID: tailObjId,
        REFERENCED_OBJECT_DOMAIN: tailObjType,
        REFERENCING_DATABASE: headDbName,
        REFERENCING_SCHEMA: headSchemaName,
        REFERENCING_OBJECT_NAME: headMatName,
        REFERENCING_OBJECT_ID: headObjId,
        REFERENCING_OBJECT_DOMAIN: headObjType,
        DEPENDENCY_TYPE: objDependencyType,
      } = el;

      if (
        typeof headDbName !== 'string' ||
        typeof headSchemaName !== 'string' ||
        typeof headMatName !== 'string' ||
        typeof headObjId !== 'number' ||
        typeof tailDbName !== 'string' ||
        typeof tailSchemaName !== 'string' ||
        typeof tailMatName !== 'string' ||
        typeof tailObjId !== 'number' ||
        typeof headObjType !== 'string' ||
        typeof tailObjType !== 'string'
      )
        throw new Error('Received sf obj representation in unexpected format');

      const head: SfObjectRef = {
        id: headObjId,
        type: headObjType,
        dbName: headDbName,
        schemaName: headSchemaName,
        matName: headMatName,
      };
      const tail: SfObjectRef = {
        id: tailObjId,
        type: tailObjType,
        dbName: tailDbName,
        schemaName: tailSchemaName,
        matName: tailMatName,
      };

      return {
        head,
        tail,
        type: parseSfObjDependencyType(objDependencyType),
      };
    });

    return dependencies;
  };

  #filterReferencedMats = async (
    sfObjDependencies: SfObjectDependency[],
    matReps: CitoMatRepresentation[]
  ): Promise<CitoMatRepresentation[]> => {
    const distinctRelationNames = sfObjDependencies.reduce(
      (accumulation: string[], val: SfObjectDependency) => {
        const localAcc = accumulation;

        const refNameHead = `${val.head.dbName}.${val.head.schemaName}.${val.head.matName}`;
        const refNameTail = `${val.tail.dbName}.${val.tail.schemaName}.${val.tail.matName}`;

        if (!localAcc.includes(refNameHead)) localAcc.push(refNameHead);

        if (!localAcc.includes(refNameTail)) localAcc.push(refNameTail);

        return localAcc;
      },
      []
    );

    const referencedMats = matReps.filter((el) =>
      distinctRelationNames.includes(el.relationName)
    );

    return referencedMats;
  };

  #buildDataDependencies = async (
    sfObjDependencies: SfObjectDependency[],
    matReps: CitoMatRepresentation[]
  ): Promise<Dependency[]> => {
    const referencedMatReps = await this.#filterReferencedMats(
      sfObjDependencies,
      matReps
    );

    const dependencies = sfObjDependencies.map((el): Dependency | undefined => {
      const headRelationName = `${el.head.dbName}.${el.head.schemaName}.${el.head.matName}`;
      const headMat = referencedMatReps.find(
        (entry) => entry.relationName === headRelationName
      );
      if (!headMat) {
        console.error(
          `Mat representation for head not found - ${headRelationName} `
        );
        return undefined;
      }

      const tailRelationName = `${el.tail.dbName}.${el.tail.schemaName}.${el.tail.matName}`;
      const tailMat = referencedMatReps.find(
        (entry) => entry.relationName === tailRelationName
      );
      if (!tailMat) {
        console.error(
          `Mat representation for tail not found - ${tailRelationName} `
        );
        return undefined;
      }

      return Dependency.create({
        id: uuidv4(),
        headId: headMat.id,
        tailId: tailMat.id,
        type: 'data',
      });
    });

    const isDependency = (obj: Dependency | undefined): obj is Dependency =>
      !!obj;
    return dependencies.filter(isDependency);
  };

  #getDbRepresentations = async (): Promise<DatabaseRepresentation[]> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Missing properties for generating sf data env');

    const dbsToIgnore = ['snowflake', 'snowflake_sample_data', 'cito'];

    const queryText = `select database_name, database_owner, is_transient, comment from cito.information_schema.databases where not array_contains(lower(database_name)::variant, array_construct(${dbsToIgnore
      .map((el) => `'${el}'`)
      .join(', ')}))`;
    const queryResult = await this.querySnowflake.execute(
      { queryText, binds: [] },
      this.#auth,
      this.#connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const dbRepresentations: DatabaseRepresentation[] = results.map((el) => {
      const {
        DATABASE_NAME: name,
        DATABASE_OWNER: ownerId,
        IS_TRANSIENT: isTransient,
        COMMENT: comment,
      } = el;

      const isComment = (val: unknown): val is string | undefined =>
        !val || typeof val === 'string';

      if (
        typeof name !== 'string' ||
        typeof ownerId !== 'string' ||
        typeof isTransient !== 'string' ||
        !['yes', 'no'].includes(isTransient.toLowerCase()) ||
        !isComment(comment)
      )
        throw new Error(
          'Received mat representation field value in unexpected format'
        );

      return {
        name,
        ownerId,
        isTransient: isTransient.toLowerCase() !== 'no',
        comment: comment || undefined,
      };
    });

    return dbRepresentations;
  };

  #generateDependencies = async (): Promise<Dependency[]> => {
    const sfObjDependencies = await this.#getSfObjectDependencies();

    const distinctRelationNames = await this.#getDistinctRelationNames(
      sfObjDependencies
    );

    const citoMatReps = await this.#getAllLineageMats(distinctRelationNames);

    const dependencies = await this.#buildDataDependencies(
      sfObjDependencies,
      citoMatReps
    );

    return dependencies;
  };

  /* Runs through snowflake and creates objects like logic, materializations and columns */
  async execute(
    req: GenerateSfEnvLineageRequestDto,
    auth: GenerateSfEnvLineageAuthDto,
    db: IDb
  ): Promise<GenerateSfEnvLineageResponse> {
    try {
      this.#connPool = db.sfConnPool;
      this.#dbConnection = db.mongoConn;
      this.#auth = auth;

      const dependenciesToCreate = await this.#generateDependencies();

      return Result.ok({
        dependenciesToCreate,
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
