package org.messaginghub.jms.example;

import jakarta.jms.*;

import java.io.InputStream;
import java.lang.IllegalStateException;
import java.util.Properties;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class HelloWorld {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (InputStream input = HelloWorld.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IllegalStateException("application.properties not found in classpath");
            }
            props.load(input);
        }
        
        String brokerURL = props.getProperty("broker.url");
        String username = props.getProperty("broker.username");
        String password = props.getProperty("broker.password");
        String queueName = props.getProperty("queue.name");
        String messagePayload = props.getProperty("message.payload");
        int maxConnections = Integer.parseInt(props.getProperty("pool.maxConnections", "1"));
        int maxSessionsPerConnection = Integer.parseInt(props.getProperty("pool.maxSessionsPerConnection", "1"));

        ActiveMQConnectionFactory factory = null;
        JmsPoolConnectionFactory poolingFactory = new JmsPoolConnectionFactory();

        try {
            factory = new ActiveMQConnectionFactory(brokerURL, username, password);
            poolingFactory.setConnectionFactory(factory);
            poolingFactory.setMaxConnections(maxConnections);
            poolingFactory.setMaxSessionsPerConnection(maxSessionsPerConnection);

            for (int i = 0; i < messagePayload.length(); ++i) {
                Connection connection = poolingFactory.createConnection();
                Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                Destination queue = session.createQueue(queueName);
                MessageProducer messageProducer = session.createProducer(queue);
                TextMessage message = session.createTextMessage("" + messagePayload.charAt(i));
                System.out.println("Sending: " + message.getText());
                messageProducer.send(message);
                message.acknowledge();

                connection.close();
            }

            Connection connection = poolingFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination queue = session.createQueue(queueName);
            MessageConsumer messageConsumer = session.createConsumer(queue);

            StringBuilder messageReceived = new StringBuilder().append("Received: ");
            for (int i = 0; i < messagePayload.length(); ++i) {
                TextMessage receivedMessage = (TextMessage) messageConsumer.receive(1);
                if (receivedMessage != null) {
                	messageReceived.append(receivedMessage.getText());
                    receivedMessage.acknowledge();
                    System.out.println(messageReceived.toString());
                } else {
                	messageReceived.setLength(0);
                    System.out.println("No message received within the given timeout.");
                    break;
                }
            }

            connection.close();
        } catch (Exception ex) {
            System.out.println("Caught exception, exiting.");
            ex.printStackTrace(System.out);
            System.exit(1);
        } finally {
            System.out.println("Shutting down the connection pool.");
            poolingFactory.stop();

            if (factory != null) {
                System.out.println("Closing underlying factory.");
                factory.close();

                System.out.println("Exiting...");
                System.exit(0);
            }
        }
    }
}
