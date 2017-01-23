<?php

namespace UrbanIndo\Yii2\Queue\Queues;

use UrbanIndo\Yii2\Queue\Queue;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitQueue extends Queue
{
    protected $connection = null;

    protected $channel = null;

    public $host = 'localhost';
    public $port = 5672;
    public $username = 'guest';
    public $password = 'guest';
    public $queue = 'yii2';

    /**
     * @return void
     */
    public function init()
    {
        parent::init();
        $this->connection = $connection = new AMQPStreamConnection($this->host, $this->port, $this->username, $this->password);
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($this->queue, false, false, false, false);
    }

    /**
     * Return next job from the queue.
     * @return Job|boolean the job or false if not found.
     */
    public function fetchJob()
    {
        $message = $this->_client->receiveMessage([
            'QueueUrl' => $this->url,
            'AttributeNames' => ['ApproximateReceiveCount'],
            'MaxNumberOfMessages' => 1,
        ]);
        if (isset($message['Messages']) && count($message['Messages']) > 0) {
            return $this->createJobFromMessage($message['Messages'][0]);
        } else {
            return false;
        }
    }

    /**
     * Post the job to queue.
     *
     * @param Job $job The job posted to the queue.
     * @return boolean whether operation succeed.
     */
    public function postJob(Job $job)
    {
        $model = $this->_client->sendMessage([
            'QueueUrl' => $this->url,
            'MessageBody' => $this->serialize($job),
        ]);
        if ($model !== null) {
            $job->id = $model['MessageId'];
            return true;
        } else {
            return false;
        }
    }

    /**
     * Delay a job to the zset. This contains implemention for database
     * @param Job $job The job to delay.
     * @param $expire Expire at.
     * @return boolean whether operation succeed.
     */
    protected function delayJob(Job $job, $expire)
    {
        $model = $this->_client->sendMessage([
            'QueueUrl' => $this->url,
            'MessageBody' => $this->serialize($job),
            'DelaySeconds' => strtotime($expire) - strtotime('now'),
        ]);
        if ($model !== null) {
            $job->id = $model['MessageId'];
            return true;
        } else {
            return false;
        }
    }

    /**
     * Delete the job from the queue.
     *
     * @param Job $job The job to be deleted.
     * @return boolean whether the operation succeed.
     */
    public function deleteJob(Job $job)
    {
        if (!empty($job->header['ReceiptHandle'])) {
            $receiptHandle = $job->header['ReceiptHandle'];
            $response = $this->_client->deleteMessage([
                'QueueUrl' => $this->url,
                'ReceiptHandle' => $receiptHandle,
            ]);
            return $response !== null;
        } else {
            return false;
        }
    }

    /**
     * Release the job.
     *
     * @param Job $job The job to release.
     * @return boolean whether the operation succeed.
     */
    public function releaseJob(Job $job)
    {
        if (!empty($job->header['ReceiptHandle'])) {
            $receiptHandle = $job->header['ReceiptHandle'];
            $response = $this->_client->changeMessageVisibility([
                'QueueUrl' => $this->url,
                'ReceiptHandle' => $receiptHandle,
                'VisibilityTimeout' => 0,
            ]);
            return $response !== null;
        } else {
            return false;
        }
    }

    /**
     * Returns the total number of all queue size.
     * @return integer
     */
    public function getSize()
    {
        return $this->db->llen($this->key);
    }

    /**
     * Purge the whole queue.
     * @return boolean
     */
    public function purge()
    {
        return $this->db->del($this->key);
    }

}
