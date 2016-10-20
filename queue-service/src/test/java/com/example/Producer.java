package com.example;

// producer produces messages and puts those in the queue
public class Producer implements Runnable{

	private final QueueService queueService;
	private final String qName;
	private String name;
	
	public Producer(String name, QueueService queueService, String qName) {
		this.name = name;
		this.queueService = queueService;
		this.qName = qName;
	}

	@Override
	public void run() {
		try{
			for(int i=0; i<10; i++) {
				String message = new StringBuilder(name).append(" message ").append(i).toString();
				System.out.println(name + " generated message : " + message);
				queueService.push(qName, message);
				
				Thread.sleep(100);
			}
		}catch(InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
