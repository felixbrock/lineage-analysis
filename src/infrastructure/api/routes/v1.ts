import { Router } from 'express';
import { appConfig } from '../../../config';
import columnsRoutes from './columns-routes';
import dashboardsRoutes from './dashboard-routes';
import dependenciesRoutes from './dependencies-routes';
import lineageRoutes from './lineage-routes';
import logicRoutes from './logic-routes';
import logicsRoutes from './logics-routes';
import materializationsRoutes from './materializations-routes';

const version = 'v1';

const v1Router = Router();

v1Router.get('/', (req, res) => res.json({ message: "Yo! We're up!" }));

v1Router.use(`/${appConfig.express.apiRoot}/${version}/lineage`, lineageRoutes);
v1Router.use(`/${appConfig.express.apiRoot}/${version}/logic`, logicRoutes);
v1Router.use(`/${appConfig.express.apiRoot}/${version}/logics`, logicsRoutes);
v1Router.use(`/${appConfig.express.apiRoot}/${version}/materializations`, materializationsRoutes);
v1Router.use(`/${appConfig.express.apiRoot}/${version}/columns`, columnsRoutes);
v1Router.use(`/${appConfig.express.apiRoot}/${version}/dependencies`, dependenciesRoutes);
v1Router.use(`/${appConfig.express.apiRoot}/${version}/dashboards`, dashboardsRoutes);

export default v1Router;
