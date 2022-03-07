import { Router } from 'express';
import { apiRoot } from '../../../config';
import lineageRoutes from './lineage-routes';

const version = 'v1';

const v1Router = Router();

v1Router.get('/', (req, res) => res.json({ message: "Yo! We're up!" }));

v1Router.use(`/${apiRoot}/${version}/lineage`, lineageRoutes);


export default v1Router;
