import serverlessExpress from '@vendia/serverless-express';
import { Application } from 'express';
import ExpressApp from './infrastructure/api/express-app';
import { appConfig } from './config';
import InternalInvokeCreateLineageController from './infrastructure/api/controllers/create-lineage-controller-internal-invoke';
import iocRegister from './infrastructure/ioc-register';
import {
  InternalInvokeType,
  parseInternalInvokeType,
  Response,
} from './shared/internal-invoke-controller';
import { CreateLineageRequestDto } from './domain/lineage/create-lineage/create-lineage';
import { parseBiTool } from './domain/value-types/bi-tool';

interface InvokeEvent {
  req: {
    catalog?: string;
    manifest?: string;
    targetOrgId: string;
    biType?: string;
  };
  auth: { jwt: string };
  internalInvokeType: string;
  [key: string]: unknown;
}

let serverlessExpressInstance: any;

const asyncTask = (): Promise<Application> => {
  const expressApp = new ExpressApp(appConfig.express);

  return expressApp.start(false);
};

const setup = async (event: InvokeEvent, context: unknown): Promise<any> => {
  const app = await asyncTask();
  serverlessExpressInstance = serverlessExpress({
    app,
  });
  return serverlessExpressInstance(event, context);
};

const getServerlessExpressInstance = async (
  event: InvokeEvent,
  context: unknown
): Promise<any> => {
  if (serverlessExpressInstance)
    return Promise.resolve(serverlessExpressInstance(event, context));

  return setup(event, context);
};

const internalInvoke = async (
  event: InvokeEvent,
  internalInvokeType: InternalInvokeType
): Promise<void | Response> => {
  if (!event.auth.jwt)
    throw new Error(
      `Cannot invoke ${internalInvokeType}. Missing auth params.`
    );

  switch (internalInvokeType) {
    case 'create-lineage': {
      if (!event.req.targetOrgId)
        throw new Error(
          `Cannot invoke ${internalInvokeType}. Missing targetOrgId.`
        );

      const createLineageController = new InternalInvokeCreateLineageController(
        iocRegister.resolve('createLineage'),
        iocRegister.resolve('getAccounts')
      );

      const req: CreateLineageRequestDto = {
        dbtCatalog: event.req.catalog,
        dbtManifest: event.req.manifest,
        targetOrgId: event.req.targetOrgId,
        biTool: event.req.biType ? parseBiTool(event.req.biType) : undefined,
      };

      const auth = { jwt: event.auth.jwt };

      const res = await createLineageController.execute({
        internalInvokeType: 'create-lineage',
        req,
        auth,
      });

      return res;
    }
    default:
      throw new Error('Unhandled invoke type provided');
  }
};

const internalInvokeHandler = async (
  event: InvokeEvent
): Promise<void | Response> => {
  try {
    const internalInvokeType = parseInternalInvokeType(
      event.internalInvokeType
    );

    await asyncTask();

    const internalInvokeResult = await internalInvoke(
      event,
      internalInvokeType
    );

    return internalInvokeResult;
  } catch (error) {
    return Promise.reject(error);
  }
};

// eslint-disable-next-line import/prefer-default-export
export const handler = async (
  event: InvokeEvent,
  context: unknown
): Promise<any> => {
  switch (event.internalInvokeType) {
    case 'create-lineage': {
      const invokeResult = await internalInvokeHandler(event);
      return invokeResult;
    }
    default: {
      const apiInstance = await getServerlessExpressInstance(event, context);
      return apiInstance;
    }
  }
};
