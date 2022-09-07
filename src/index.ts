import ExpressApp from './infrastructure/api/express-app';
import { appConfig } from './config';

const expressApp = new ExpressApp(appConfig.express);

expressApp.start(true);

// // import serverlessExpress from '@vendia/serverless-express';
// // import { Application } from 'express';
// // import ExpressApp from './infrastructure/api/express-app';
// // import { appConfig } from './config';
// import { Application } from 'express';
// import CreateLineageController from './infrastructure/api/controllers/create-lineage-controller';
// import iocRegister from './infrastructure/ioc-register';
// import {
//   InternalInvokeType,
//   parseInternalInvokeType,
// } from './infrastructure/shared/internal-invoke-controller';
// import { CreateLineageRequestDto } from './domain/lineage/create-lineage';
// import ExpressApp from './infrastructure/api/express-app';
// import { appConfig } from './config';

// // let serverlessExpressInstance: any;

// const asyncTask = (): Promise<Application> => {
//   const expressApp = new ExpressApp(appConfig.express);

//   return expressApp.start(false);
// };

// // const setup = async (event: any, context: any): Promise<any> => {
// //   const app = await asyncTask();
// //   serverlessExpressInstance = serverlessExpress({
// //     app,
// //   });
// //   return serverlessExpressInstance(event, context);
// // };

// // const getServerlessExpressInstance = async (
// //   event: any,
// //   context: any
// // ): Promise<any> => {
// //   if (serverlessExpressInstance)
// //     return Promise.resolve(serverlessExpressInstance(event, context));

// //   return setup(event, context);
// // };

// const internalInvoke = async (
//   event: any,
//   internalInvokeType: InternalInvokeType
// ): Promise<any> => {
//   if (!event.auth.jwt)
//     throw new Error(
//       `Cannot invoke ${internalInvokeType}. Missing auth params.`
//     );

//   const createLineageController = new CreateLineageController(
//     iocRegister.resolve('createLineage'),
//     iocRegister.resolve('getAccounts'),
//     iocRegister.resolve('dbo')
//   );

//   switch (internalInvokeType) {
//     case 'create-lineage': {
//       if (
//         !event.req.catalog ||
//         !event.req.manifest ||
//         !event.req.targetOrganizationId
//       )
//         throw new Error(
//           `Cannot invoke ${internalInvokeType}. Missing req params.`
//         );

//       const req: CreateLineageRequestDto = {
//         catalog: event.req.catalog,
//         manifest: event.req.manifest,
//         targetOrganizationId: event.req.targetOrganizationId,
//         biType: event.req.biType,
//         lineageCreatedAt: event.req.lineageCreatedAt,
//         lineageId: event.req.lineageId,
//       };

//       const auth = { jwt: event.auth.jwt };

//       const res = await createLineageController.execute({
//         internalInvokeType: 'create-lineage',
//         req,
//         auth,
//       });

//       return res;
//     }
//     default:
//       throw new Error('Unhandled invoke type provided');
//   }
// };

// const event = {
//   internalInvokeType: 'create-lineage',
//   auth: {
//     jwt: 'eyJraWQiOiJrVkFzOUJSbGE5M3IrUStZdm5tS1I1UTlcL21WcEVyQlphRUNXZitFdnpVWT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzbzAyOW5qaTE1NHYwYm0xMDloa3Zrb2k1aCIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoic3lzdGVtLWludGVybmFsXC9zeXN0ZW0taW50ZXJuYWwiLCJhdXRoX3RpbWUiOjE2NjI1ODAxNDYsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC5ldS1jZW50cmFsLTEuYW1hem9uYXdzLmNvbVwvZXUtY2VudHJhbC0xXzBaOEpoRmo4eiIsImV4cCI6MTY2MjU4Mzc0NiwiaWF0IjoxNjYyNTgwMTQ2LCJ2ZXJzaW9uIjoyLCJqdGkiOiJlZjA5NTJlMy0zNTNkLTRhNzQtYmJlYi01MWQzMzg0NWY0NDUiLCJjbGllbnRfaWQiOiIzbzAyOW5qaTE1NHYwYm0xMDloa3Zrb2k1aCJ9.SB3k-7OZDjYoB1YYVnU3nPLPsms8DmfsMCxm8r8GsPXmFiBF2ZvDNIajYAfRWP-HMATfjximnybp-O8LliTpMBCUWTU3rajk5wA5QVx_Cte8_XQ7MLAdMybMPxv9VSCNrS5CAox69nEfMje8A7wYbs5--48dz3R_o5vYT1QJFe75d3djlID_F3g23MmZEU0bTshF7v389vKZSaECEo8-SOVZxC-52MmHjDrjSpGTIPlViVk-TBXCqAxZV75DkgFFy5I2PW6rkmp7NkhDmQOq6wPG930Z2Ib1Md3fDK6sjilxYlv5FdXW23GPX6Fh8gqPZ1PSWhJRbtJroHIi5R-vhg',
//   },
//   req: {
//     catalog: 'abc',
//     manifest: 'abc',
//     targetOrganizationId: 'abc',
//     biType: 'abc',
//     lineageCreatedAt: 'abc',
//     lineageId: 'abc',
//   },
// };
// const context = {};

// console.log('xxxxxxxxxxxxx', event);
// console.log('yyyyyyyyyyyyy', context);

// const internalInvokeType = parseInternalInvokeType(event.internalInvokeType);

// asyncTask()
//   .then(() => internalInvoke(event, internalInvokeType))
//   .then((res) => console.log(res))
//   .catch((error) => console.error(error));
