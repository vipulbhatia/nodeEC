var mysql = require('mysql'),
    stomp = require('stomp'),
    fs = require('fs'),
    redis = require('redis'),
    async = require('async');

//process.__defineGetter__('stdout', function() { return fs.createWriteStream(__dirname + '/abc.log', {flags:'w'}) });

var opts = {
    host     : '155.16.55.234',
    port     : 3306,
    user     : 'foglight',
    password : '$Dell2013!',
    database : 'test'
};

// Set debug to true for more verbose output.
// login and passcode are optional (required by rabbitMQ)
var stomp_args = {
    port: 61613,
    host: '155.16.55.168',
    debug: true,
    login: 'admin',
    passcode: 'admin',
};

var client = null;
var redisClient = null;
var connection = null;
var connectingToDB = 0;
var connectingToAMQ = 0;
var connectingToRedis = 0;

var connectToDB = function() {
    console.log('calling db connect');
    console.log('calling db connect: inside while');
    connection = mysql.createConnection(opts);
    connection.on('error', function(err) {
        console.log('error from mysql, reconnection...' + err.toString());
        setTimeout(connectToDB, 5000);
    });
    connection.connect(function(err) {
      if(err) {
        console.error('error connecting: ' + err.stack.toString());
        setTimeout(connectToDB, 5000);
      } else {
          console.log('connected as id ' + connection.threadId.toString());
          connecting = 0;
          if(client === null) {
            connectToRedis();
          }
        }
    });
}

var done = () => {
    console.log('done');
}

var connectToRedis = () => {
    redisClient = redis.createClient();

    redisClient.on('error', (err) => {
        console.error('redisClient error:', err);
    });

    redisClient.on('ready', () => {
        console.error('redisClient connected...');
        var q1 = mysql.format('select event_handle, uuid from events where status not like ?', ['%closed']);
        connection.query(q1, (err, rows, fields) => {
            if(err) console.error('select query error:', err);
            async.each(rows, (row, done)) => {
                redisClient.set(row.eventhandle, 1, (err, res) => {
                    done();
                });
            }, (err) => {
                if(err) console.error(err);
                connectToAMQBroker();
            });
        });
    });
}

var connectToAMQBroker = function() {
    console.log('connecting to AMQ Broker...');
    client = new stomp.Stomp(stomp_args);
    client.connect();

    client.on('connected', function() {
        client.subscribe(oheaders, message_callback);
        console.log('Connected');
        connectingToAMQ = 0;
    });

    client.on('error', function(error_frame) {
        console.log('error with AMQ Broker: ' + error_frame);
        client.disconnect();
        //client = null;
    });

    client.on('disconnected', function() {
        console.log('disconnect with AMQ Broker: reconnecting');
        //client.disconnect();
        //client = null;
        setTimeout(connectToAMQBroker, 5000);
    });

    client.on('receipt', function(receipt) {
        //console.log("RECEIPT: " + receipt);
    });

};

connectToDB();

// 'activemq.prefetchSize' is optional.
// Specified number will 'fetch' that many messages
// and dump it to the client.
var oheaders = {
    destination: '/queue/test_nodejs',
    ack: 'client',
//    'activemq.prefetchSize': '10'
};

var messages = 0;

function message_callback(body, headers) {
    console.log('Message Callback Fired!');
    console.log("Got message: " + headers['message-id']);
    client.ack(headers['message-id']);
    msg = JSON.parse(body);
    var q2 = mysql.format('insert into events(data, hostname, source, Event_Handle) values(?,?,?,?)', [body, msg.hostname, msg.from, msg.eventhandle]);
    redisClient.exists(q1, function(err, reply) {
        //console.log(q1);
        if(err) {
            console.log('error in mysql read: ' + err.toString());
            client.send({
                'destination': '/queue/test_nodejs',
                'persistent' : 'true',
                'body' : body
            }, true);
            if(err.fatal === true && connecting === 0) {
                connecting = 1;
                connectToDB();
            }
            return;
        };
        if(reply == 0) {
            redisClient.set(msg.eventhandle, 1, (err, reply) => {
                if(err) console.error(err);
                else connection.query(q2, function(err, rows, fields) {
                    if(err) {
                        console.log('error in mysql write: ' + err.toString());
                        client.send({
                            'destination': '/queue/test_nodejs',
                            'persistent' : 'true',
                            'body' : body
                        }, true);
                        if(err.fatal === true && connecting === 0) {
                            connecting = 1;
                            connectToDB();
                        }
                        return;
                    };
                    console.log('row added to MySQL');
                });
            });
        } else {
            console.log('duplicate detected');
        }

    });
}

process.on('SIGINT', function() {
    client.disconnect();
    console.log('exiting...');
    process.exit();
});
