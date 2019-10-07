var express = require('express');
const request = require('request');
const debug = require('debug')('dot');
var rp = require('request-promise');
var MongoClient = require("mongodb").MongoClient;
var async = require("async");
var fs = require('fs');
var moment = require('moment');

var app = express();
var db;
var previousThreadsList = [];
let flaggedThreads = {};
var callNumber = 0; // simple debug tool

const consoleExec = true; // Log to console every execution?
const consoleWarn = true; // Log warnings?

const fileUrl = 'https://000.00';
const threadsApiUrl = 'https://000.00/0/threads.json';
const postsApiUrl = 'https://000.00/0/res/';
const dbUrl = "mongodb://localhost:27017/mongo";

// This program works pretty damn fast, don't make 'em laggy
const asyncLimit = 6; // The maximum number of async operations (thread requests) at a time.
const maxAttempts = 2; // How many times attempt to download failed threads (0 - 999..)
const bump_limit = 485; // For thread delete detector. Put less than real number
const timeToDelete = 60 * 20; // For thread delete detector (s)
const updateInterval = 1000 * 20; // (ms)
const errorInterval = 200; // Wait if connection fails (ms)
const SemaphoreLimit = 3; /* Number of attempts.
0: updateInterval >> 2 min; 3: updateInterval = 20-40 sec.
See Semaphore code below. If thread blocked, restart delay:
                [D = updateInterval X SemaphoreLimit] */



// MongoDB connection pool
MongoClient.connect(dbUrl, { useNewUrlParser: true, poolSize: 10, useUnifiedTopology: true }, function(err, database){

    if (err) {
        console.log("@ Mongodb fail");
        throw err;
    } else {
        console.log("DB connected")
    }

    db = database.db("mongo");
    appStart();

});


function appStart() {

    // Check threadsList for records
    db.collection('threadsList').findOne({}, function (err, result) {
        try {

            if (err) console.log(err);

            if (result !== null) {

                let bdTimestamp = moment.unix(result.timestamp);
                let timeNow = moment();
                //console.log( bdTimestamp.from(timeNow) );
                console.log('Last thread list from DB: ' + (bdTimestamp.from(timeNow)) );
                try {
                    previousThreadsList = result.threadsList;
                } catch (e) {
                    console.log('@ ThreadsList collection not found. Make it');
                    console.log(e);
                }

            } else {
                console.log('DB memory is clean');
                //////////////////// Clean threads collection
            }

            startCycle()

        } catch (e) {
            console.log('@ App start error');
            console.log(e);
        }


    });

    // Run general cycle
    function startCycle() {
        SemaphoreVar.reset();
        setInterval(Semaphore, updateInterval);
        Semaphore();
    }

}

// Prevent race condition
// In case of slow thread Semaphore will lock next before old one completely done or enough time has passed.
class SemaphoreVar {
    // Set() keeps thread alive
    static set() {this.count = SemaphoreLimit;}

    static dec() {this.count--;}
    // Reset() lets run next thread
    static reset() {this.count = 0;}
    // Ins() inspector
    static ins() {console.log(this.count);}
    // State() returns boolean value, thereby allowing or blocking next thread.
    static state() {return !!!this.count;}
}
//SemaphoreVar.set();
//SemaphoreVar.reset();
//SemaphoreVar.ins();
function Semaphore() {
    SemaphoreVar.ins();///////////////////////
    if (SemaphoreVar.state()) {
        SemaphoreVar.set();
        threadsListGrabber();
        callNumber++;
    } else {
        SemaphoreVar.dec();
        console.log("======================== REFUSED ==================");
    }
}


function threadsListGrabber() {
    rp(threadsApiUrl).then(body => {
        try {
            let threadsList = JSON.parse(body).threads;
            if (!!threadsList) {
                threadsListManager(threadsList);
            } else {
                console.log('@ Threads API error');
                SemaphoreVar.set();
                setTimeout(threadsListGrabber, (errorInterval*10));
            }
        } catch (e) {
            if (consoleWarn) console.log('@ Threads Parse error');
            SemaphoreVar.set();
            setTimeout(threadsListGrabber, (errorInterval*10));
        }
    }).catch(err => {
        if (consoleWarn) console.log('@ Threads Connect error');
        SemaphoreVar.set();
        setTimeout(threadsListGrabber, (errorInterval*10));
    });

    /*fs.readFile('./test/threads.json', 'utf8', function(err, data) {
        if (err) throw err;
        let threadsList = JSON.parse(data).threads;
        threadsListManager(threadsList);
    });*/

}


function threadsListManager(threadsList) {

    let removeList = []; // List of deleted threads
    let delThreadsList = []; // List of manual deleted threads for save
    flaggedThreads.a = []; // DO NOT links them together
    flaggedThreads.u = [];
    flaggedThreads.n = [];
    let log_str = "";
    let count = {}; // Counts for log & statistics
    count.a = count.u = count.n = 0;

    // Sort by Num
    threadsList.sort(function(a, b){
        return a.num - b.num;
    });


    log_str += '[0] Had '+previousThreadsList.length;
    log_str += '\t\tTook '+threadsList.length;


    (function(A, B) {

        B.forEach(function(thread) {
            let num = thread.num;

            // Find B element in A array
            let x = A.findIndex(function(element) {
                if (element.num === num) {return true;}
            });

            // Setting up flags
            if (x === -1) {
                thread.flag = 'A';
                count.a++;
                flaggedThreads.a.push(thread.num);
            } else {
                if ((thread.lasthit === A[x].lasthit)&&(thread.posts_count === A[x].posts_count)) {
                    thread.flag = 'N';
                    count.n++;
                    flaggedThreads.n.push(thread.num);
                } else {
                    thread.flag = 'U';
                    count.u++;
                    flaggedThreads.u.push(thread.num);
                }
                // Remove for find optimization
                A.splice(x, 1);
            }

        });

    })(previousThreadsList, threadsList);

    //console.log(flaggedThreads);

    // Рокировка
    let toRemove = previousThreadsList;
    previousThreadsList = threadsList;

    let timeNow = Math.floor(new Date() / 1000);

    // threadsList to DB
    db.collection('threadsList').updateOne({}, {$set: {threadsList, timestamp: timeNow}}, { upsert: true }, function(err){
        if (err) {
            console.log("@ DB threadsList insert error" + err);
        }
    });

    log_str += '\t\tDelete '+toRemove.length;
    log_str += "\t\tUpdate "+count.u+"\t\tInsert "+count.a+"\t\tIgnore "+count.n+' ('+callNumber+')';
    if (consoleExec) debug(log_str);

    // Catch every manual deleted thread
    toRemove.forEach(function(thread) {
        removeList.push(thread.num);
        if ((thread.posts_count<bump_limit) && (timeNow-thread.lasthit)<=timeToDelete) {
            //console.log('Этот тред был удалён вручную. Последнее сообщение ' + (timeNow-thread.lasthit) + ' секунд назад.');
            if (consoleExec) console.log("Удаленный тред!");
            delThreadsList.push(thread.num);
        }
    });

    // Replace manual deleted threads from DB to deletedThreads collection
    if (delThreadsList.length > 0) {

        db.collection('threads').find({ num : { $in: delThreadsList } }).toArray(function (err, result) {

            if (err) {
                console.log(err);
            } else {
                let bulk = db.collection('deletedThreads').initializeUnorderedBulkOp();

                result.forEach(function(thread) {
                    bulk.insert( { num: thread.num, posts: thread.posts} );
                });

                bulk.execute(function (err, result) {
                    if (err) console.log(err);
                    console.log('!!!! Сохраняем треды!');
                    delRemovedThreds();
                });
            }


        });
        /////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////////////

        /*delThreadsList.forEach(function (num) {
            db.collection('threads').findOne({num}, function(err, result){
                if (err) {
                    console.log("@ Take manual deleted thread from DB error" + err);
                }
                /////////////////////////////////////////////////////
                db.collection('deletedThreads').insertOne({ 'posts':result.posts, num: num }, function(err2, result2){
                    if (err2) {
                        console.log("@ Insert manual deleted threads to DB error" + err);
                    }
                    //console.log(num);
                    console.log('Deleted threads saved');
                });

            });
        });*/
    } else {
        if (removeList.length > 0) {
            delRemovedThreds();
        }
    }

    // Remove all deleted threads
    function delRemovedThreds() {
            let bulk = db.collection('threads').initializeUnorderedBulkOp();

            bulk.find( { num : { $in: removeList } } ).remove();

            bulk.execute(function (err, result) {
                if (err) {
                    console.log('@ remove threads error');
                    console.log(err);
                }
                //console.log(result);
            });
    }

    //requiredThreads = requiredThreads.slice(0,10);
    debug('[1] Threads list set. '+'('+callNumber+')');
    postsGrabber();

}


async function postsGrabber() {

    let threadsToInsert = []; // Array for add/update threads (full)
    let attemptsCounter = 0;

    // Filter out only A/U threads for download
    let requiredThreads = flaggedThreads.a.concat(flaggedThreads.u);
    //console.log(requiredThreads);

    /*requiredThreads.forEach(function (thread) {
        let num = thread;
        let file = './test/' + num + '.json';

        let data = fs.readFileSync(file, 'utf8');
        let posts = JSON.parse(data).threads[0].posts;
        //console.log(posts);
        threadsToInsert.push({num, posts});
    });

    //console.log(threadsToInsert);
    postsManager(threadsToInsert);*/


    function getPosts (thread) {

        let abortedThreads = [];

        async.mapLimit(thread, asyncLimit, function(thread, callback){

                let num = thread;
                let fullUrl = postsApiUrl + num + '.json';

                rp(fullUrl).then(body => {
                    try {
                        let posts = JSON.parse(body).threads[0].posts;

                        if (!!posts) {
                            threadsToInsert.push({num, posts});
                            //console.log('ok '+num);
                            callback(null, 1);
                        } else {
                            if (consoleWarn) console.log('@ Posts API error '+num);
                            abortedThreads.push(thread);
                            callback(null, 'api abort');
                        }
                    } catch (e) {
                        /////////////////if (consoleWarn) console.log('@ Posts Parse error '+num);
                        abortedThreads.push(thread);
                        callback(null, 'parse abort');
                    }
                }).catch(err => {
                    if (consoleWarn) console.log('@ Connect Posts error '+num);
                    abortedThreads.push(thread);
                    // callback should return something, otherwise it will poison parallel exe.
                    callback(null, 'connect error');
                });
                },
        (err, results) => {
            if (err) console.log(err);
            //console.log(results);

            //if (abortedThreads.length > 0) {console.log(abortedThreads);}

            attemptsCounter++;
            // Warning: Recursion
            if (abortedThreads.length > 0) {
                if (attemptsCounter <= maxAttempts) {
                    //console.log('======================= RETRY '+(abortedThreads.length)+' thread(s)');
                    setTimeout(getPosts, errorInterval, abortedThreads);
                    //getPosts(abortedThreads);
                } else {
                    console.log('======================= ПОПЫТКИ всё. Незагружено '+(abortedThreads.length)+' тред(ов)');
                    postsManager(threadsToInsert);
                }
            } else {
                //console.log('Okay');
                postsManager(threadsToInsert);
            }

        });

    }

    getPosts(requiredThreads);

}


function postsManager(threadsToInsert) {
    let delPosts = []; // Array for deleted posts
    let files = []; // Array for files of deleted posts

    debug('[2]  PostGrabber end. '+'('+callNumber+')');

    if (flaggedThreads.u.length > 0) {

        let temp = flaggedThreads.u;

        db.collection('threads').find({ num : { $in: temp } }).toArray(function (err, oldThreads) {

            if (err) {
                console.log('@ finding u threads error');
                return 1;
            }

            //console.log(oldThreads);
            //console.log('Получен ответ');

            debug('[3] PostManager db. '+'('+callNumber+')');
            //console.log('');

            let count_e = 0;

            for (let oldThread of oldThreads) {

                // Find
                let u = threadsToInsert.findIndex(function(thread) {
                    if (thread.num === oldThread.num) {return true;}
                });

                if (u === -1) {
                    //console.log('@ Not found. Probably didnt downloaded. SKIP');
                    count_e++;
                    /////////////////////////
                    /////////////////////////
                    /////////////////////////
                    continue;
                }
                //console.log(threadsToInsert[u]);

                // Compare Logic
                (function(A, B) {

                    // Posts iterator
                    B.forEach(function (newPost) {
                        // Find B element in A array
                        let x = A.findIndex(function(oldPost) {

                            if ((oldPost.num === newPost.num)&&(oldPost.comment === newPost.comment)) {
                                return true;
                            } else if (oldPost.files.length > 0) {

                                try {
                                    if (JSON.stringify(oldPost.files) === JSON.stringify(newPost.files)) {
                                        return true;
                                    }
                                } catch (e) {
                                    console.log("@ field files not found. API wrong?");
                                }

                            } else {
                                //console.log("Сработка");
                                //console.log(oldPost);
                            }

                        });

                        if (x === -1) {
                            // Skip new post
                        } else {
                            A.splice(x, 1);
                        }

                    });

                })(oldThread.posts, threadsToInsert[u].posts);

                // Detected posts remains in oldThread(s) array.
                if (oldThread.posts.length > 0) {
                    oldThread.posts.forEach(function (post) {
                        delPosts.push(post);
                    });
                }


            }

            if (count_e > 0) {
                console.log('@ '+count_e+' threads not found. Probably didnt downloaded. SKIP');
                console.log(oldThreads.length);
                console.log(threadsToInsert.length);
                //console.log(temp);
                //console.log(flaggedThreads.u.length);
            }


            //console.log(oldThreads);
            ////console.log(threadsToInsert);
            //console.log(delPosts);

            if (delPosts.length > 0) {
                let bulk = db.collection('deletedPosts').initializeUnorderedBulkOp();

                delPosts.forEach(function(post) {

                    bulk.find( { num : post.num } ).upsert().update( { $set: post } );

                    if (post.files.length > 0) {
                        post.files.forEach(function (file) {
                            let link = fileUrl + file.path;
                            files.push({link: link, name: file.fullname});
                        })
                    }
                });

                bulk.execute(function (err, result) {
                    if (err) console.log(err);
                    console.log((delPosts.length) + ' постов сохранено');
                });
            }


        });
    }


    if (threadsToInsert.length > 0) {
        let bulk = db.collection('threads').initializeUnorderedBulkOp();

        threadsToInsert.forEach(function(thread, i) {
            bulk.find( { num : thread.num } ).upsert().update( { $set: {'posts':thread.posts} } );
        });

        bulk.execute(function (err, result) {
            if (err) console.log(err);
            //console.log(result);
        });
    }

    debug('[4] End. '+'('+callNumber+')');
    SemaphoreVar.reset();
    ////////////////////////////////////////
    ////filesDownloader();
    ///////////////////////////////////////



}
