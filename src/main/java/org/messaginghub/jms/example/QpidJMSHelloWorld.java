package org.messaginghub.jms.example;

import jakarta.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class QpidJMSHelloWorld {

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = null;
        JmsPoolConnectionFactory poolingFactory = new JmsPoolConnectionFactory();

        try {
            Context context = new InitialContext();
            String username = context.getEnvironment().get("broker.username").toString();
            String password = context.getEnvironment().get("broker.password").toString();
            String messagePayload = context.getEnvironment().get("message.payload").toString();
            int maxConnections = Integer.parseInt(context.getEnvironment().get("pool.maxConnections").toString());
            int maxSessionsPerConnection = Integer.parseInt(context.getEnvironment().get("pool.maxSessionsPerConnection").toString());

            factory = (ConnectionFactory) context.lookup("myFactoryLookup");

            poolingFactory.setConnectionFactory(factory);
            poolingFactory.setMaxConnections(maxConnections);
            poolingFactory.setMaxSessionsPerConnection(maxSessionsPerConnection);

            for (int i = 0; i < messagePayload.length(); ++i) {
                Connection connection = poolingFactory.createConnection(username, password);
                Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                Destination queue = (Destination) context.lookup("myQueueLookup");
                MessageProducer messageProducer = session.createProducer(queue);
                TextMessage message = session.createTextMessage("" + messagePayload.charAt(i));
                System.out.println("Sending: " + message.getText());
                messageProducer.send(message);
                message.acknowledge();

                connection.close();
            }

            Connection connection = poolingFactory.createConnection(username, password);
            connection.start();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination queue = (Destination) context.lookup("myQueueLookup");
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

            System.out.println("Exiting...");
            System.exit(0);            
        }
    }
}
