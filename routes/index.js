var express = require('express');
var router = express.Router();
var MongoClient = require("mongodb").MongoClient;
//require('crawler');
let db;
const dbUrl = "mongodb://localhost:27017/mongo";
const prefix = "https://2ch.hk";

/* GET home page. */

// MongoDB connection pool
MongoClient.connect(dbUrl, { useNewUrlParser: true, poolSize: 10, useUnifiedTopology: true }, function(err, database){

  if (err) {
    console.log("@ Mongodb fail"); throw err;
  } else {
    //console.log("DB connected")
  }

  db = database.db("mongo");

});

router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

// thread list
router.get('/list', function(req, res, next) {

  try {

    db.collection('threadsList').findOne({}, function (err, result) {
      if (err) {
        throw err;
      }

      if (result.length !== 0) {
        let resas = result.threadsList;
        res.render('threadsList', { title: 'MongoDB: Mongo/threadList contains:', threads: resas });
      } else {
        res.send('DB memory is clean');
      }

    });

  } catch (e) {
    console.log(e);
  }

});

// deleted posts
router.get('/posts', function(req, res, next) {

  try {

    db.collection('deletedPosts').find({}).sort({ num: -1 }).toArray(function (err, result) {
      if (err) {
        throw err;
      }

      if (result.length !== 0) {
        //console.log(result.length);
        res.render('posts', { title: 'Deleted posts:', posts: result, prefix: prefix });
      } else {
        res.send('DB memory is clean');
      }

    });

  } catch (e) {
    console.log(e);
  }

});

// deleted threads
router.get('/threads', function(req, res, next) {

  try {

    db.collection('deletedThreads').find({}).sort({ num: -1 }).toArray(function (err, result) {
      if (err) {
        throw err;
      }

      if (result.length !== 0) {

        res.render('threads', { title: 'Deleted threads:', threads: result});
      } else {
        res.send('DB memory is clean');
      }

    });

  } catch (e) {
    console.log(e);
  }

});

module.exports = router;
