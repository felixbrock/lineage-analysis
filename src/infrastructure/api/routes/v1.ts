import { Router } from 'express';
import { apiRoot } from '../../../config';
import tableRoutes from './table-routes';

const version = 'v1';

const v1Router = Router();

v1Router.get('/', (req, res) => res.json({ message: "Yo! We're up!" }));

v1Router.use(`/${apiRoot}/${version}/table`, tableRoutes);


export default v1Router;
