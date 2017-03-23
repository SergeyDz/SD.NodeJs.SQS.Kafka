var log = require('./Log.js').Log;
var sqs = require('sqs');
var args = process.argv.slice(2);

log.info('started');
log.info(args[2]);

 
var queue = sqs({
	access: args[0],
	secret: args[1],
	region: args[2] // defaults to us-east-1 
});

function bail(err) {
  console.error(err);
  process.exit(1);
}


var kafka = require('kafka-node'),
        Producer = kafka.Producer,
        KeyedMessage = kafka.KeyedMessage,
        client = new kafka.Client("10.1.1.231:2181/kafka", "entities", { sessionTimeout: 3000 }),
        producer = new Producer(client),
        km = new KeyedMessage('key', 'message');
        

function pullSQS(kafkaProducer)
{
    queue.pull(args[3], function(message, callback) {
        log.info(message);  
             
        // produce kafka here
        var   payloads = [
            { topic: 'entities', messages: messages, partition: 0 }
        ];
        
        kafkaProducer.send(payloads, function(err, data) {
            log.info(data);
        });
      
        callback(); // we are done with this message - pull a new one 
                    // calling the callback will also delete the message from the queue 
    });
}

producer.on('error', function(err) { log.error(err); })

producer.on('ready', function() {
  pullSQS(producer);
});