package ru.jusaf.springkafkademo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.io.Closeable
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import kotlin.concurrent.thread

@SpringBootApplication
class SpringKafkaDemoApplication

fun main(args: Array<String>) {
    runApplication<SpringKafkaDemoApplication>(*args)
    val topic = "spring-kafka-demo"
    val producer = MyProducer(topic)
    // new Thread(lambda).start()
    thread {
        (1..100).forEach { i ->
            producer.send(i.toString(), "Hello from MyProducer!")
            TimeUnit.SECONDS.sleep(5)
        }
    }
    val consumer = MyConsumer(topic)
    consumer.consume { record ->
        println("Got key: ${record.key()}, value: ${record.value()}")
    }
    TimeUnit.MINUTES.sleep(10)
    producer.close()
    consumer.close()
}

class MyConsumer(private val topic: String) : Closeable {
    private val consumer = getConsumer()

    private fun getConsumer(): KafkaConsumer<String, String> {
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        // groupId - это идентификатор consumer. Если мы хотим получать одни и те же сообщения для разных consumer-ах,
        // то мы должны указать разные groupId
        props[ConsumerConfig.GROUP_ID_CONFIG] = "groupId"
        props[ConsumerConfig.CLIENT_ID_CONFIG] = "clientId"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        val consumer = KafkaConsumer<String, String>(props)
        consumer.subscribe(listOf(topic))
        return consumer
    }

    fun consume(recordConsumer: Consumer<ConsumerRecord<String, String>>) {
        // new Thread(lambda).start()
        thread {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(1))
                records.forEach { record ->
                    recordConsumer.accept(record)
                }
            }
        }
    }

    override fun close() {
        consumer.close()
    }
}

class MyProducer(private val topic: String) : Closeable {
    private val producer = getProducer()

    private fun getProducer(): KafkaProducer<String, String> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.CLIENT_ID_CONFIG] = "clientId"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        return KafkaProducer<String, String>(props)
    }

    fun send(key: String, value: String) {
        // Метод send асинхронный, он возвращает Future, мы можем не дождаться отправки сообщения и завершить нашу программу
        producer.send(ProducerRecord(topic, key, value)).get()
    }

    override fun close() {
        // Если вы вручную создаёте продюсера, не забывайте закрывать его, это ресурс
        producer.close()
    }
}