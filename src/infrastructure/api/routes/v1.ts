import { Router } from 'express';
import { apiRoot } from '../../../config';
import columnsRoutes from './columns-routes';
import dependenciesRoutes from './dependencies-routes';
import lineageRoutes from './lineage-routes';
import logicsRoutes from './logic-routes';
import materializationsRoutes from './materializations-routes';

const version = 'v1';

const v1Router = Router();

v1Router.get('/', (req, res) => res.json({ message: "Yo! We're up!" }));

v1Router.use(`/${apiRoot}/${version}/lineage`, lineageRoutes);
v1Router.use(`/${apiRoot}/${version}/logic`, logicsRoutes);
v1Router.use(`/${apiRoot}/${version}/materializations`, materializationsRoutes);
v1Router.use(`/${apiRoot}/${version}/columns`, columnsRoutes);
v1Router.use(`/${apiRoot}/${version}/dependencies`, dependenciesRoutes);

export default v1Router;
