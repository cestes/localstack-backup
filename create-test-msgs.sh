awslocal sqs send-message --queue-url http://localhost:4566/000000000000/TestQueue1 --message-body test1.1
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/TestQueue1 --message-body test1.2
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/TestQueue1 --message-body test1.3
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/TestQueue2 --message-body test2.1
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/TestQueue2 --message-body test2.2
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/fifo-test-queue.fifo --message-body 3.1 --message-group-id abc  
