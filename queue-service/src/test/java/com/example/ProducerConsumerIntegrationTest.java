package com.example;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.CreateQueueRequest;

public class ProducerConsumerIntegrationTest {

	private static String qName = "Queue1";
	private static QueueService queueService = new InMemoryQueueService( new InMemoryQueueFactory());
	private int producerPoolSize = 100;
	private int consumerPoolSize = 100;
	private ExecutorService producers = Executors.newFixedThreadPool(producerPoolSize);
    private ExecutorService consumers = Executors.newFixedThreadPool(consumerPoolSize);
      
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		ProducerConsumerIntegrationTest pc = new ProducerConsumerIntegrationTest();
		
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		queueService.createQueue(createQueueRequest);
		
		for (int i = 0; i < pc.producerPoolSize ; i++) {
        	Producer producer = new Producer("Producer " + i, queueService , qName);
        	pc.producers.submit(producer);
		} 
		
		for (int i = 0; i < pc.consumerPoolSize ; i++) {
            Consumer consumer = new Consumer("Consumer " + i, queueService , qName);
            pc.consumers.submit(consumer);
        }
		
		pc.producers.awaitTermination(3, TimeUnit.SECONDS);
		pc.consumers.awaitTermination(3, TimeUnit.SECONDS);
		System.out.println("Forcing shutdown...");
		pc.consumers.shutdownNow();
		pc.producers.shutdownNow();
		System.exit(1);
   	}

}
