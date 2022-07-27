import {
    Db,
    DeleteResult,
    Document,
    FindCursor,
    InsertManyResult,
    InsertOneResult,
    ObjectId,
  } from 'mongodb';
  import sanitize from 'mongo-sanitize';
  
  import {
    DashboardQueryDto,
    IDashboardRepo,
  } from '../../domain/dashboard/i-dashboard-repo';
  import {
    Dashboard,
    DashboardProperties,
  } from '../../domain/entities/dashboard';
  
  interface DashboardPersistence {
    url?: string;
    name?: string;
    materialisation: string;
    column: string; 
    _id: ObjectId;
    lineageId: string;
    columnId: string,
    matId: string;
  }
  
  interface DashboardQueryFilter {
    url?: string;
    name?: string;
    materialisation: string;
    column: string; 
    lineageId: string;
    columnId: string,
    matId: string;
  }
  
  const collectionName = 'dashboard';
  
  export default class DashboardRepo implements IDashboardRepo {
    findOne = async (
      id: string,
      dbConnection: Db
    ): Promise<Dashboard | null> => {
      try {
        const result: any = await dbConnection
          .collection(collectionName)
          .findOne({ _id: new ObjectId(sanitize(id)) });
  
        if (!result) return null;
  
        return this.#toEntity(this.#buildProperties(result));
      } catch (error: unknown) {
        if (typeof error === 'string') return Promise.reject(error);
        if (error instanceof Error) return Promise.reject(error.message);
        return Promise.reject(new Error('Unknown error occured'));
      }
    };
  
    findBy = async (
      dashboardQueryDto: DashboardQueryDto,
      dbConnection: Db
    ): Promise<Dashboard[]> => {
      try {
        if (!Object.keys(dashboardQueryDto).length)
          return await this.all(dbConnection);
  
        const result: FindCursor = await dbConnection
          .collection(collectionName)
          .find(this.#buildFilter(sanitize(dashboardQueryDto)));
        const results = await result.toArray();
  
        if (!results || !results.length) return [];
  
        return results.map((element: any) =>
          this.#toEntity(this.#buildProperties(element))
        );
      } catch (error: unknown) {
        if (typeof error === 'string') return Promise.reject(error);
        if (error instanceof Error) return Promise.reject(error.message);
        return Promise.reject(new Error('Unknown error occured'));
      }
    };
  
    #buildFilter = (
      dashboardQueryDto: DashboardQueryDto
    ): DashboardQueryFilter => {
      const filter: DashboardQueryFilter = {
        lineageId: dashboardQueryDto.lineageId,
        materialisation: dashboardQueryDto.materialisation,
        column: dashboardQueryDto.column, 
        columnId: dashboardQueryDto.columnId,
        matId: dashboardQueryDto.matId,

      };
  
      if (dashboardQueryDto.url) filter.url = dashboardQueryDto.url;
      if (dashboardQueryDto.name) filter.name = dashboardQueryDto.name;
  
      return filter;
    };
  
    all = async (dbConnection: Db): Promise<Dashboard[]> => {
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
        if (typeof error === 'string') return Promise.reject(error);
        if (error instanceof Error) return Promise.reject(error.message);
        return Promise.reject(new Error('Unknown error occured'));
      }
    };
  
    insertOne = async (
      dashboard: Dashboard,
      dbConnection: Db
    ): Promise<string> => {
      try {
        const result: InsertOneResult<Document> = await dbConnection
          .collection(collectionName)
          .insertOne(this.#toPersistence(sanitize(dashboard)));
  
        if (!result.acknowledged)
          throw new Error('Dashboard creation failed. Insert not acknowledged');
  
  
        return result.insertedId.toHexString();
      } catch (error: unknown) {
        if (typeof error === 'string') return Promise.reject(error);
        if (error instanceof Error) return Promise.reject(error.message);
        return Promise.reject(new Error('Unknown error occured'));
      }
    };
  
    insertMany = async (
      dashboards: Dashboard[],
      dbConnection: Db
    ): Promise<string[]> => {
  
      try {
        const result: InsertManyResult<Document> = await dbConnection
          .collection(collectionName)
          .insertMany(
            dashboards.map((element) => this.#toPersistence(sanitize(element)))
          );
  
        if (!result.acknowledged)
          throw new Error(
            'Dashboard creations failed. Inserts not acknowledged'
          );
  
        return Object.keys(result.insertedIds).map((key) =>
          result.insertedIds[parseInt(key, 10)].toHexString()
        );
      } catch (error: unknown) {
        if (typeof error === 'string') return Promise.reject(error);
        if (error instanceof Error) return Promise.reject(error.message);
        return Promise.reject(new Error('Unknown error occured'));
      }
    };
  
    deleteOne = async (id: string, dbConnection: Db): Promise<string> => {
      try {
        const result: DeleteResult = await dbConnection
          .collection(collectionName)
          .deleteOne({ _id: new ObjectId(sanitize(id)) });
  
        if (!result.acknowledged)
          throw new Error('Dashboard delete failed. Delete not acknowledged');
  
        return result.deletedCount.toString();
      } catch (error: unknown) {
        if (typeof error === 'string') return Promise.reject(error);
        if (error instanceof Error) return Promise.reject(error.message);
        return Promise.reject(new Error('Unknown error occured'));
      }
    };
  
    #toEntity = (properties: DashboardProperties): Dashboard =>
      Dashboard.create(properties);
  
    #buildProperties = (
      dashboard: DashboardPersistence
    ): DashboardProperties => ({
      // eslint-disable-next-line no-underscore-dangle
      id: dashboard._id.toHexString(),
      lineageId: dashboard.lineageId,
      materialisation: dashboard.materialisation,
      column: dashboard.column,
      columnId: dashboard.columnId,
      matId: dashboard.matId,
      url: dashboard.url,
      name: dashboard.name

    });
  
    #toPersistence = (dashboard: Dashboard): Document => ({
      _id: ObjectId.createFromHexString(dashboard.id),
      ineageId: dashboard.lineageId,
      materialisation: dashboard.materialisation,
      column: dashboard.column,
      columnId: dashboard.columnId,
      matId: dashboard.matId,
      url: dashboard.url,
      name: dashboard.name
    });
  }
  