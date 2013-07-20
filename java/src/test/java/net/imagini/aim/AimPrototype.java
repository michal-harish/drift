package net.imagini.aim;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import joptsimple.internal.Strings;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class AimPrototype {
    
    static ConsumerConnector consumer;

    public static void main(String[] args) throws InterruptedException, IOException {

        MockServer server = new MockServer(4000);
        server.start();

        try {
            Socket socket = new Socket(
                InetAddress.getByName("localhost"), //10.100.11.239 
                4000
            );
            final Pipe pipe = MockServer.type.getConstructor(OutputStream.class).newInstance(socket.getOutputStream());

            Properties consumerProps = new Properties();
            consumerProps.put("zk.connect", "zookeeper-01.stag.visualdna.com");
            consumerProps.put("groupid", "aim-kafka-loader-dev");
            System.out.println("Connecting to Kafka..");
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
            @SuppressWarnings("serial")
            KafkaStream<Message> stream = consumer.createMessageStreams(new HashMap<String,Integer>() {{
                put("prod_conversions", 1);
            }}).get("prod_conversions").get(0);
            System.out.println("Consuming messages..");

            ConsumerIterator<Message> it = stream.iterator();
            ObjectMapper jsonMapper = new ObjectMapper();
            while(it.hasNext()) {
                MessageAndMetadata<Message> metaMsg = it.next();
                Message message = metaMsg.message();
                ByteBuffer buffer = message.payload();
                byte [] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                //System.out.println(new String(bytes));
                JsonNode json = jsonMapper.readValue(bytes, JsonNode.class);
                try {
                    new Event(
                        Long.valueOf(json.get("timestamp").getValueAsText()), 
                        InetAddress.getByName(json.get("client_ip").getTextValue()), 
                        json.get("event_type").getTextValue(), 
                        json.get("useragent").getTextValue(), 
                        json.get("country_code").getTextValue(),
                        json.get("region_code").getTextValue(),
                        "??? ??", 
                        json.get("campaignId").getTextValue(),
                        json.get("url").getTextValue(), 
                        UUID.fromString(json.get("extradata").get("vdna_widget_mc").getTextValue()), 
                        !Strings.isNullOrEmpty(json.get("userUid").getTextValue())
                    ).write(pipe);
                } catch(UnknownHostException e1) {
                    System.out.println("Invalid client_ip: " + json.get("client_ip"));
                } catch(Exception e) {
                   e.printStackTrace();
                }
            }
            pipe.close();
            socket.close();
        }
        catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
           ex.printStackTrace();
        }

        server.interrupt();

    }
}
