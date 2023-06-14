const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "learn-pup-sub-Kafka",
  brokers: ["103.169.35.246:9092"],
});

async function check_topic() {
  const admin = kafka.admin();
  await admin.connect();
  const list_topic = await admin.listTopics();
  console.log(list_topic);
  await admin.disconnect();
}

check_topic();

async function check_partitions() {
    const admin = kafka.admin();
    await admin.connect();
    const list_partitions = await admin.fetchTopicMetadata({
        topics: ['test-topic']
    });
    console.log(list_partitions.topics[0].partitions);
    await admin.disconnect();
}
check_partitions();

async function create_partition(topic, partition_id) {
    const admin = kafka.admin();
    await admin.connect();
    await admin.createPartitions({
        topicPartitions: [{
            topic: 'test-topic',
            count: 4
        }]
    });
    await admin.disconnect();
}

// create_partition();

async function pub(partition_id) {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: "test-topic",
    messages: [{ 
        //key: `key${partition_id}`,
        partition: partition_id,
        value: "Hello KafkaJS user!" 
    }],
  });
  console.log(`send message to partition id: ${partition_id} successfully`);
  await producer.disconnect();
}

pub(3);