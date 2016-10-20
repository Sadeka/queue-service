package com.example;

import java.util.TimerTask;

public class ScheduledTask extends TimerTask{

	MessageQueue messageQueue;
	
	public ScheduledTask(MessageQueue messageQueue) {
		this.messageQueue = messageQueue;
	}

	@Override
	public void run() {
		messageQueue.refreshQueue();
	}

}
