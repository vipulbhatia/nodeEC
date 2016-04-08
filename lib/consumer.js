var mysql      = require('mysql');
var stomp = require('stomp');
var fs = require('fs');

process.__defineGetter__('stdout', function() { return fs.createWriteStream(__dirname + '/abc.log', {flags:'w'}) });

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
var connection = null;
var connectingToDB = 0;
var connectingToAMQ = 0;

var connectToDB = function() {
    console.log('calling db connect');
        console.log('calling db connect: inside while');
        connection = mysql.createConnection(opts);
        connection.on('error', function(err) {
            console.log('error from mysql, reconnection...' + err.toString());
            setTimeout(connectToDB, 5000);
        });
        connection.connect(function(err) {
          if (err) {
            console.error('error connecting: ' + err.stack.toString());
            setTimeout(connectToDB, 5000);
          } else {
              console.log('connected as id ' + connection.threadId.toString());
              connecting = 0;
              if(client === null) {
                connectToAMQBroker();
            }
          }
        }); 
};

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
    q1 = mysql.format('select count(*) as result from events where hostname=? and source=? and Event_Handle=?', [msg.hostname, msg.from, msg.eventhandle]);
    q2 = mysql.format('insert into events(data, hostname, source, Event_Handle) values(?,?,?,?)', [body, msg.hostname, msg.from, msg.eventhandle]);
    connection.query(q1, function(err, rows, fields) {
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
        console.log(rows[0].result);
        if(rows[0].result == 0) {
            connection.query(q2, function(err, rows, fields) {
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