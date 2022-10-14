import serverlessExpress from '@vendia/serverless-express';
import { Application } from 'express';
import ExpressApp from './infrastructure/api/express-app';
import { appConfig } from './config';
import InternalInvokeCreateLineageController from './infrastructure/api/controllers/create-lineage-controller-internal-invoke';
import iocRegister from './infrastructure/ioc-register';
import {
  InternalInvokeType,
  parseInternalInvokeType,
} from './infrastructure/shared/internal-invoke-controller';
import { CreateLineageRequestDto } from './domain/lineage/create-lineage/create-lineage';

let serverlessExpressInstance: any;

const asyncTask = (): Promise<Application> => {
  const expressApp = new ExpressApp(appConfig.express);

  return expressApp.start(false);
};

const setup = async (event: any, context: any): Promise<any> => {
  const app = await asyncTask();
  serverlessExpressInstance = serverlessExpress({
    app,
  });
  return serverlessExpressInstance(event, context);
};

const getServerlessExpressInstance = async (
  event: any,
  context: any
): Promise<any> => {
  if (serverlessExpressInstance)
    return Promise.resolve(serverlessExpressInstance(event, context));

  return setup(event, context);
};

const internalInvoke = async (
  event: any,
  internalInvokeType: InternalInvokeType
): Promise<any> => {
  if (!event.auth.jwt)
    throw new Error(
      `Cannot invoke ${internalInvokeType}. Missing auth params.`
    );

  const createLineageController = new InternalInvokeCreateLineageController(
    iocRegister.resolve('createLineage'),
    iocRegister.resolve('getAccounts'),
    iocRegister.resolve('dbo')
  );

  switch (internalInvokeType) {
    case 'create-lineage': {
      if (
        !event.req.catalog ||
        !event.req.manifest ||
        !event.req.targetOrganizationId
      )
        throw new Error(
          `Cannot invoke ${internalInvokeType}. Missing req params.`
        );

      const req: CreateLineageRequestDto = {
        catalog: event.req.catalog,
        manifest: event.req.manifest,
        targetOrganizationId: event.req.targetOrganizationId,
        biType: event.req.biType,
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

const internalInvokeHandler = async (event: any): Promise<void> => {
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
export const handler = async (event: any, context: any): Promise<any> => {
 
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
