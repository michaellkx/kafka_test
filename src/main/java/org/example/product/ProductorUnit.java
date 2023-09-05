package org.example.product;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProductorUnit {

    private final static String myTopic = "demo";    //主题
    public static volatile KafkaProducer<String,String> producer;

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.3.128:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        int a = 100;
        while(a <101 && a > 0){
            a--;
            int finalA = a;
            new Thread(){
                @Override
                public void run(){
                    try{
                        producer.send(new ProducerRecord<String, String>(myTopic, "myKey", finalA+""));  //发送
                        System.out.println("执行成功"+finalA+"");
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }

            }.start();
        }




    }



}
