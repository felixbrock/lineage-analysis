import { Router } from 'express';
import app from '../../ioc-register';
import ReadLogicsController from '../controllers/read-logics-controller';

const logicsRoutes = Router();

const readLogicsController = new ReadLogicsController(
  app.resolve('readLogics'),
  app.resolve('getAccounts'),
  app.resolve('db')
);

logicsRoutes.get('/', (req, res) => {
  readLogicsController.execute(req, res);
});

export default logicsRoutes;
