package net.imagini.aim;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;

import joptsimple.internal.Strings;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import net.imagini.aim.node.EventsSchema;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class KafkaEvents {

    static ConsumerConnector consumer;

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws InterruptedException, IOException, IllegalAccessException {

        try {
            Socket socket = new Socket(
                InetAddress.getByName("localhost"), //10.100.11.239 
                4000
            );
            final Pipe pipe = new PipeLZ4(socket.getOutputStream(), Pipe.Protocol.LOADER);
            AimSchema schema = new EventsSchema();
            pipe.write(schema.serialize());

            Properties consumerProps = new Properties();
            consumerProps.put("zk.connect", "zookeeper-01.stag.visualdna.com");
            consumerProps.put("groupid", "aim-kafka-loader-dev");
            System.out.println("Connecting to Kafka " + consumerProps.getProperty("zk.connect") +"..");
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
            KafkaStream<Message> stream = consumer.createMessageStreamsByFilter(new Whitelist("prod_conversions,prod_pageviews,prod_interactions"),1).get(0);
            ConsumerIterator<Message> it = stream.iterator();
            ObjectMapper jsonMapper = new ObjectMapper();
            boolean started = false;
            int count = 0;
            while(it.hasNext()) {
                if (!started) {
                    System.out.println("Consuming messages..");
                    started = true;
                }
                MessageAndMetadata<Message> metaMsg = it.next();
                Message message = metaMsg.message();
                ByteBuffer buffer = message.payload();
                byte [] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                JsonNode json = jsonMapper.readValue(bytes, JsonNode.class);
                try {
                    Long timestamp = Long.valueOf(json.get("timestamp").getValueAsText());
                    Boolean user_quizzed = !Strings.isNullOrEmpty(json.get("userUid").getTextValue()) && json.get("userUid").getTextValue().length() > 10;

                    pipe.write(timestamp);
                    pipe.write(schema.def("client_ip").convert(json.get("client_ip").getTextValue()));
                    pipe.write(json.get("event_type").getTextValue());
                    pipe.write(json.get("useragent").getTextValue());
                    pipe.write(Arrays.copyOfRange((json.get("country_code").getTextValue() + "??").getBytes(), 0, 2));
                    pipe.write(Arrays.copyOfRange((json.get("region_code").getTextValue() +"???").getBytes(), 0, 3));
                    pipe.write("??? ??");
                    pipe.write(json.get("campaignId").getTextValue());
                    pipe.write(json.get("url").getTextValue());
                    pipe.write(schema.def("user_uid").convert(json.get("userUid").getTextValue()));
                    pipe.write(user_quizzed);
 
                    if (++count == 10 * 1000000) {
                        break;
                    }
                } catch(Exception e) {
                    continue;
                }
            }
            pipe.close();
            socket.close();
        }
        catch (IOException | IllegalArgumentException | SecurityException ex) {
           ex.printStackTrace();
        }

    }
}
