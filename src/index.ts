// import dotenv from 'dotenv';

// dotenv.config();

// // eslint-disable-next-line import/first
// import ExpressApp from './infrastructure/api/express-app';
// // eslint-disable-next-line import/first
// import { appConfig } from './config';

// const expressApp = new ExpressApp(appConfig.express);

// expressApp.start();

import express from 'express';
import { spawn } from 'child_process';

const app = express();
const port = 3000;
app.get('/', (req, res) => {
 
 let dataToSend: any;
 const python = spawn('python', ['sql-parser.py']);

 python.stdout.on('data', (data) => {
  console.log('Pipe data from python script ...');
  dataToSend = data.toString();
 });

 python.on('close', (code) => {
 console.log(`child process close all stdio with code ${code}`);
 // send data to browser
 res.send(dataToSend);
 });
 
});
app.listen(port, () => console.log(`Example app listening on port 
${port}!`));