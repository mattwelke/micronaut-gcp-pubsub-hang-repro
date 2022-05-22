package pubsub.pipeline.app;

import com.google.pubsub.v1.PubsubMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.gcp.pubsub.annotation.PubSubListener;
import io.micronaut.gcp.pubsub.annotation.Subscription;
import jakarta.annotation.PreDestroy;

@PubSubListener
public class MyPubSubListener {

    private static final Logger LOG = LoggerFactory.getLogger(MyPubSubListener.class);

    @Subscription("topic-sub")
    public void onMessage(PubsubMessage message) {     
        var messageId = message.getMessageId();
        
        LOG.info("Received message with ID " + messageId + ". Invoking message processor.");

        try {
            Thread.sleep(9000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        LOG.info("Message with ID " + messageId + " contents: " + message.getData() + ".");

        LOG.info("Processor finished processing message with ID " + messageId + ".");
    }

    @PreDestroy
    void onShutdown() {
        // TODO: stop the underlying subscription gracefully
        LOG.info("PreDestroy");
    }
}
