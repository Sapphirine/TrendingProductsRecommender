const express = require('express');
const index = require('./routes/index');

const app = express();
app.use(express.static(__dirname + '/public/onepage'));
app.use(express.static(__dirname + '/public'));
app.use('/', index);

module.exports = app;
