#================================================================================================
# localstack-backup.py
#
# This program will make a backup of and restore your localstack environment, if you don't 
# have the 'pro' version that includes environment persistance. It currently makes a snapshot
# of the following:
#     - S3 buckets and each object in the bucket (binary objects seem to work)
#     - SQS queues and all messages in queues.
#     - SNS topics and their subscriptions. DLQ subscriptions are supported.
#
# ASSUMPTIONS:
#     - When restoring, it assumes the environmnet is *completely* empty.
#     - All work is done in region 'us-east-1'
#     - The program will create 6 .pickle files in the directory where the it is run when making
#       the backup. The restore must be run in the same directory so it can find these files.
#     - All services are expected to be at the default LocalStack URL (see localstack_endpoint_url)
#     - Fancy, custom ARNs are not supported - use the defaults that LocalStack gives you!
#     - Error checking and informational messages are minimal as I want it done quickly! I'll
#       work on that as we go and if people need this for more than a short duration.
#     - When backing up SQS, the tool will delete each message as it is read from the queue.
#       This ensures we don't have multiple copies of each message in the backup.
#================================================================================================


import boto3
import pickle
import random
import string

localstack_endpoint_url = "http://localhost:4566"

def gen_rand_str(strlen):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(strlen))

#================================================================================================
# This function will take every object in every bucket and save it in a pickle file.
#================================================================================================
def backup_s3():
    # Assume the worst!
    success = False

    # Connect to the S3 service
    try:
        s3 = boto3.client(
            's3',
            endpoint_url = localstack_endpoint_url,
            region_name = 'us-east-1'
        )
        s3_conn = True
    except:
        print("\n***Unable to connect to LocalStack S3 service - make sure everything is running***\n")
        s3_conn = False
        success = False # I know, redundant, but just to be sure!

    if s3_conn:
        # Get a list of all S3 buckets
        buckets = s3.list_buckets()

        # Initialize a list to store the objects
        objects = []

        # Create and save a list of bucket names. Required to deal with empty buckets
        bucket_list = []
        for bucket in buckets['Buckets']:
            bucket_list.append(bucket['Name'])
        with open('s3_buckets.pickle', 'wb') as f:
            pickle.dump(bucket_list, f)

        # Loop through each bucket
        for bucket in buckets['Buckets']:
            print(f"Working on bucket {bucket['Name']}")
            # Get the contents of each bucket
            contents = s3.list_objects(Bucket=bucket['Name'])
            # Loop through the contents of each bucket
            for obj in contents['Contents']:
                print(f"    Working on object {obj['Key']}")
                # Get the object details
                obj_details = s3.get_object(Bucket=bucket['Name'], Key=obj['Key'])
                # Add the object details to the list
                objects.append({
                    'bucket_name': bucket['Name'],
                    'object_key': obj['Key'],
                    'object_body': obj_details['Body'].read()
                })

        # Save the objects in a pickle file
        with open('s3_objects.pickle', 'wb') as f:
            pickle.dump(objects, f)

    return(success)

#================================================================================================
# This function will backup every message from every SQS queue
#================================================================================================
def backup_sqs():
    success = False

    # Connect to the SQS service
    try:
        sqs = boto3.client(
            'sqs',
            endpoint_url = localstack_endpoint_url,
            region_name = 'us-east-1'
        )
        sqs_conn = True
    except:
        print("\n***Unable to connect to LocalStack SQS service - make sure everything is running***\n")
        sqs_conn = False
        success = False # I know, redundant, but just to be sure!

    if sqs_conn:
        # Get a list of all SQS queues and save it
        queues = sqs.list_queues()
        queueUrls = queues['QueueUrls']
        with open('sqs_queues.pickle', 'wb') as f:
            pickle.dump(queueUrls, f)

        # Iterate over all queues and retrieve the messages
        all_messages = []
        for queue_url in queues['QueueUrls']:
            print(f"Processing queue: {queue_url}")
            # how many messages in this queue?
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            msg_count = int(response['Attributes']['ApproximateNumberOfMessages'])
            print(f"Expecting {msg_count} messages")

            # Use the receive_message method to retrieve messages from the queue and add to a list
            if msg_count > 0:
                for i in range(msg_count):
                    response = sqs.receive_message(QueueUrl=queue_url,MaxNumberOfMessages=1)
                    message = response['Messages'][0]
                    all_messages.append({"queue":queue_url,"body":message['Body']})
                    sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=message['ReceiptHandle'])
            
        # save the messages
        with open('sqs_messages.pickle', 'wb') as f:
            pickle.dump(all_messages, f)

    return(success)

#================================================================================================
# This function will back up all of the SNS topics, their associated subscriptions, and
# any dead letter queues associated with the subscriptions.
#================================================================================================
def backup_sns():

    success = False

    try:
        sns = boto3.client('sns', endpoint_url = localstack_endpoint_url, region_name = 'us-east-1')
        sns_conn = True
    except:
        print("Error connecting to SNS!!!")
        sns_conn = False

    if sns_conn:
        topic_list = []

        response = sns.list_topics()
        topics = response['Topics']
        if len(topics) > 0:
            # first let's store the list of topic ARNs
            for topic in topics:
                topic_list.append(topic['TopicArn'])
            with open('sns_topics.pickle', 'wb') as f:
                pickle.dump(topic_list, f)

            subscription_list = []

            # next lets save any subscriptions
            for topic in topic_list:
                response = sns.list_subscriptions_by_topic(TopicArn=topic)
                subscriptions = response['Subscriptions']
                for subscription in subscriptions:
                    this_sub = {"topicARN": topic,
                                "subARN": subscription['SubscriptionArn'],
                                "protocol": subscription['Protocol'],
                                "endpoint": subscription['Endpoint']}
                    if 'RawMessageDelivery' in subscription and 'RedrivePolicy' in subscription['RawMessageDelivery']:
                        redrive_policy = subscription['RawMessageDelivery']['RedrivePolicy']
                        this_sub['DLQARN'] = redrive_policy['deadLetterTargetArn']
                    subscription_list.append(this_sub)

            # and finally pickle the subscriptions
            with open('sns_subs.pickle', 'wb') as f:
                pickle.dump(subscription_list, f)

    return success

#================================================================================================
# master backup function -- calls individual services
#================================================================================================
def backup():
    status = backup_s3()
    status = backup_sqs()
    status = backup_sns()

#================================================================================================
# restores the S3 buckets and all objects
#================================================================================================
def restore_s3():
    # Assume the worst!
    success = False

    # Connect to the S3 service
    try:
        s3 = boto3.client(
            's3',
            endpoint_url = localstack_endpoint_url,
            region_name = 'us-east-1'
        )
        s3_conn = True
    except:
        print("\n***Unable to connect to LocalStack S3 service - make sure everything is running***\n")
        s3_conn = False
        success = False # I know, redundant, but just to be sure!
    
    if s3_conn:
        # first, let's recreate the buckets
        bucket_list = pickle.load(open("s3_buckets.pickle", "rb"))
        if len(bucket_list) > 0:
            for bucket in bucket_list:
                print(f"Restoring bucket {bucket}")
                s3.create_bucket(Bucket=bucket)

            # next recreate the objects
            object_list = pickle.load(open("s3_objects.pickle", "rb"))
            if len(object_list) > 0:
                print(f"Restoring {len(object_list)} objects")
                for object in object_list:
                    s3.put_object(Bucket=object['bucket_name'], Key=object['object_key'], Body=object['object_body'])

    return success
#================================================================================================
# restores all SQS queues and the messages that were in the queues
#================================================================================================
def restore_sqs():
    success = False
    print("Restoring SQS")
    # Connect to the SQS service
    try:
        sqs = boto3.client(
            'sqs',
            endpoint_url = localstack_endpoint_url,
            region_name = 'us-east-1'
        )
        sqs_conn = True
    except:
        print("\n***Unable to connect to LocalStack SQS service - make sure everything is running***\n")
        sqs_conn = False
        success = False # I know, redundant, but just to be sure!

    # get the list of queue names from the pickle file and recreate them.
    if sqs_conn:
        queue_list = pickle.load(open("sqs_queues.pickle", "rb"))
        if len(queue_list) > 0:
            for queue in queue_list:
                # we only need the name which is at the end of the url
                queue_name = queue.split("/")[-1]
                print(f"Recreating {queue_name}")
                # make sure anything ending in ".fifo" is recreated as a fifo queue
                if queue_name.endswith(".fifo"):
                    response = sqs.create_queue(
                        QueueName=queue_name,
                        Attributes={
                            'FifoQueue': 'true',
                            'ContentBasedDeduplication': 'true'
                        }
                    )
                else:
                    response = sqs.create_queue(QueueName=queue_name)

        # now recreate the messages
        message_list = pickle.load(open("sqs_messages.pickle", "rb"))
        if len(message_list) > 0:
            print(f"Restoring {len(message_list)} messages")
            for message in message_list:
                # deal with fifo queues properly - they require 2 extra parameters.
                # I picked totally random values here, that could possibly break
                # something, but hopefully not.
                if message['queue'].endswith(".fifo"):
                    response = sqs.send_message(
                        QueueUrl = message['queue'],
                        MessageBody = message['body'],
                        MessageGroupId = gen_rand_str(3),
                        MessageDeduplicationId = gen_rand_str(3)
                    )
                else:
                    response = sqs.send_message(
                        QueueUrl=message['queue'],
                        MessageBody=message['body']
                    )

    return success


#================================================================================================
# restores all SNS topics and the subscrptions to each topic.
#================================================================================================
def restore_sns():
    success = False
    try:
        sns = boto3.client('sns', endpoint_url = localstack_endpoint_url, region_name = 'us-east-1')
        sns_conn = True
    except:
        print("Error connecting to SNS!!!")
        sns_conn = False

    if sns_conn:
        # get a list of all the topics
        topic_list = pickle.load(open("sns_topics.pickle", "rb"))

        if len(topic_list) > 0:
            # create each topic
            for topic in topic_list:
                topic_name = topic.split(":")[-1]
                print(f"Restoring SNS topic: {topic_name}")
                response = sns.create_topic(Name=topic_name)

        # get a list of all subscriptions
        subscriptions = pickle.load(open("sns_subs.pickle", "rb"))

        if len(subscriptions) > 0:
            # restore each subscription
            print(f"Restoring {len(subscriptions)} SNS subscriptions")
            for subscription in subscriptions:
                # if the subscription is part of a DLQ, then we have to use the redrive policy to make it work
                if subscription.has_key("DLQARN"):
                    response = sns.subscribe(
                        TopicArn = subscription['TopicARN'],
                        Protocol = subscription['Protocol'],
                        Endpoint = subscription['Endpoint'],
                        Attributes = {
                            'RedrivePolicy': '{ "deadLetterTargetArn": "' + subscription['DLQARN'] + '", "maxReceiveCount": "5"}'
                        }
                    )
                else: # just a normal subscription
                    response = sns.subscribe(
                        TopicArn = subscription['TopicARN'],
                        Protocol = subscription['Protocol'],
                        Endpoint = subscription['Endpoint']
                    )
    return success


#================================================================================================
# master restore function -- calls individual services
#================================================================================================
def restore():
    status = restore_s3()
    status = restore_sqs()
    status = restore_sns()


#================================================================================================
# main program
#================================================================================================
def main():
    print("\n\n\n")
    print("What would you like to do?")
    print("    [b]ackup LocalStack state")
    print("    [r]estore LocalStack state\n")
    choice = input("Please enter your choice 'b' or 'r': ").lower()

    if choice not in ["b","r"]:
        print("\n***Invalid choice - exiting***")
    elif choice == "b":
        status = backup()
    else:
        status = restore()

    print("\nThanks for using the LocalStack backup tool!\n")

#================================================================================================
#================================================================================================
if __name__ == "__main__":
    main()
