package com.example;

public abstract class FileQueueService implements QueueService {
  // File based Queue Service to enable message communication among various producers-consumers 
  // residing in different JVMs. Tentative solution: Inter-process communication via socket programming or REST API.
}
