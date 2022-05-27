import { Router } from 'express';
import app from '../../ioc-register';
import ReadColumnsController from '../controllers/read-columns-controller';

const columnsRoutes = Router();

const readColumnsController = new ReadColumnsController(
  app.resolve('readColumns'),
  app.resolve('getAccounts'),
  app.resolve('db')
);

columnsRoutes.get('/', (req, res) => {
  readColumnsController.execute(req, res);
});

export default columnsRoutes;
