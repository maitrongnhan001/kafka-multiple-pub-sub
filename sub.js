const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "learn-pup-sub-Kafka",
  brokers: ["103.169.35.246:9092"],
//   sasl: {
//     mechanism: "plain",
//     username: "mtn_kafka",
//     password: "@#afaew@4213312@$$#%#",
//   },
});

const consumer = kafka.consumer({ groupId: "test-group" });

async function Sub(partition_id) {
  await consumer.connect();
  await consumer.subscribe({ topic: "test-topic", partition: 0, fromBeginning: true });

  await consumer.run({
    partitionsConsumedConcurrently: 4,
    eachMessage: async ({ topic, partition, message }) => {
      console.log(topic);
      console.log({
        value: message.value.toString()
      });
      console.log(partition);
    },
  });
}

