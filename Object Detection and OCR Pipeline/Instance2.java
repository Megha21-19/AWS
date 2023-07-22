package org.cloudproj.app;

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.ListIterator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
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
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.TextDetection;
import javax.jms.*;
import com.amazon.sqs.javamessaging.*;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.AmazonSQS;


class MyListener implements MessageListener {
	 
    @Override
    public void onMessage(Message message) {
    	
        try {
        	
         // The credentials added are as ## , and are active , simply execute the jar file.
         Regions region = Regions.US_EAST_1;

         String bucketName = "njit-cs-643";
         
         // amazon client
         final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
         
         // rekognition
         AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
 
            
        // Bucket objects listing
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;

            
            result = s3.listObjectsV2(req);    
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) 
            {    
               String m = (String) ((TextMessage) message).getText().toString();
     		   if(objectSummary.getKey().contains(m))
     		   {
     			  // System.out.println("Received: " + ((TextMessage) message).getText());
     			   String photo = objectSummary.getKey();
     			   //text rekognition of the image from the queue
     			   DetectTextRequest request = new DetectTextRequest()
     			              .withImage(new Image()
     			              .withS3Object(new S3Object()
     			              .withName(photo)
     			              .withBucket(bucketName)));            			    
     			   try {
     			          DetectTextResult result1 = rekognitionClient.detectText(request);
     			          List<TextDetection> textDetections = result1.getTextDetections();
     			          if (!textDetections.isEmpty()) {
     			        	  	System.out.print("Text Detected lines and words for:  " + photo + " ==> ");
     			        	  for (TextDetection text: textDetections) {
     			        		
     			        		 System.out.print("  Text Detected: " + text.getDetectedText() + " , Confidence: " + text.getConfidence().toString());
     			        		 System.out.println();
     			        		  }			              
     			         }
     			     } catch(AmazonRekognitionException e) {
     			    	  System.out.print("oops in the catch");
     			          e.printStackTrace();
     			      }
     		   }
            }
            
        } catch (JMSException e) {
           System.out.println("Please run the instance 1 first..");
        }
    }
}





public class App 
{
    public static void main( String[] args ) throws Exception
    {SpringApplication.run(App.class, args);
        Regions region = Regions.US_EAST_1;

        String bucketName = "njit-cs-643";

        try {
            AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                 .withRegion(region)
                 .build();

        try {

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
        
        // Create a consumer for the 'MyQueue'.
        MessageConsumer consumer = session.createConsumer(queue);
             
        // Instantiate and set the message listener for the consumer.
        consumer.setMessageListener(new MyListener());
         
        // Start receiving incoming messages.
        connection.start();
        
        Thread.sleep(10000);

        }catch(Exception e) {
            System.out.println("Please run the instance 1, the program will wait for the queue to have elements.");

            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
            new ProviderConfiguration(),
        AmazonSQSClientBuilder.defaultClient()
            );

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
        
        // Create a consumer for the 'MyQueue'.
        MessageConsumer consumer = session.createConsumer(queue);
             
        // Instantiate and set the message listener for the consumer.
        consumer.setMessageListener(new MyListener());
         
        // Start receiving incoming messages.
        connection.start();
        
        Thread.sleep(10000);

        }   
        }
        catch (AmazonServiceException e) {
            System.out.println("Please run the instance 1 first.");
        }
}
}