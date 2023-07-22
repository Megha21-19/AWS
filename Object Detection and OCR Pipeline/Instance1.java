package org.cloudproj.app;

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.ListIterator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.AmazonServiceException;

import com.amazonaws.services.rekognition.model.BoundingBox;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Instance;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.Parent;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;


public class App
{
    public static void main( String[] args )  throws JMSException
    {   
        SpringApplication.run(App.class, args);

        Regions region = Regions.US_EAST_1;

        String bucketName = "njit-cs-643";

        try {
        
        // amazon client
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
        
        // rekognition
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
        
        // Create a new connection factory with all defaults (credentials and region) set automatically
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
            new ProviderConfiguration(),
        AmazonSQSClientBuilder.defaultClient()
        );

        // Create the connection.
        SQSConnection connection = connectionFactory.createConnection();
        
        // Get the wrapped client
        AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
 
        // Create an Amazon SQS FIFO queue named MyQueue.fifo, if it doesn't already exist
        if (!client.queueExists("MyQueue.fifo")) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put("FifoQueue", "true");
            attributes.put("ContentBasedDeduplication", "true");
            client.createQueue(new CreateQueueRequest().withQueueName("MyQueue.fifo").withAttributes(attributes));
        }

        // Create the nontransacted session with AUTO_ACKNOWLEDGE mode
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // Create a queue identity and specify the queue name to the session
        Queue queue = session.createQueue("MyQueue.fifo");
         
        // Create a producer for the 'MyQueue'
        MessageProducer producer = session.createProducer(queue);

        // ------------------------------------------

        // Bucket objects listing
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
            do {
                result = s3.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) 
                {
                	String filename = objectSummary.getKey();
                   
                    DetectLabelsRequest request = new DetectLabelsRequest()
                            .withImage(new Image().withS3Object(new S3Object().withName(filename).withBucket(bucketName)))
                            .withMaxLabels(10).withMinConfidence(75F);
                    try {                    	
                        DetectLabelsResult result1 = rekognitionClient.detectLabels(request);
                        List<Label> labels = result1.getLabels();

                        Hashtable<String, Integer> numbers = new Hashtable<String , Integer>();

                        for (Label label : labels) 
                        {
                            
                            if(label.getName().equals("Car") & label.getConfidence()>90) 
                            {	
                            	 System.out.print("Detected labels for:  " + filename +" => ");
                            	 numbers.put(label.getName(), Math.round(label.getConfidence()));
                            	 System.out.print("Label: " + label.getName() + " ,");
                                 System.out.print("Confidence: " + label.getConfidence().toString() + "\n");
                                 System.out.println("Pushed to sqs.");
                                 TextMessage message = session.createTextMessage(objectSummary.getKey());
                            	 message.setStringProperty("ccMsgGroup", "Default");
                            	 producer.send(message);
                            	 System.out.println("JMS Message " + message.getJMSMessageID());
                            }                                     
                        }  


                        } 
                    catch (AmazonRekognitionException e) {
                            e.printStackTrace();
                    }       	
                }
                String token = result.getNextContinuationToken();
                //System.out.println("Next Continuation Token: " + token);
                req.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            }
        } 
}

