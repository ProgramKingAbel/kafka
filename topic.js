const Kafka = require("kafkajs").Kafka
run();
async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": ["ip:9092"]
        });

        const admin = kafka.admin();
        console.log("connecting...")
        await admin.connect()
        console.log("connected!")
        await admin.createTopics({
            "topics": [{
                "topic": "Users",
                "numPartitions": 2
            }]
        })
        console.log("created successfully");
        await admin.disconnect();

    }
    catch(ex) {
        console.error(`sth bad happened ${ex}`)
    }
    finally {
        process.exit(0);
    }

}