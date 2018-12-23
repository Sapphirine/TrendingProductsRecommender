const express = require('express');

const router = express.Router();

//router.get('/', (req, res) => {
//  res.send('It works!!');
//});

router.get('/', express.static('../public/onepage/index.html'))

module.exports = router
