<?php

namespace UrbanIndo\Yii2\Queue\Queues;

use UrbanIndo\Yii2\Queue\Job;
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
        $job = null;

        $callback = function($msg) use (&$job) {
            $job = $this->deserialize($msg);
        };

        $this->channel->basic_consume($this->queue, '', false, true, false, false, $callback);

        while(count($this->channel->callbacks) && !$job) {
            $this->channel->wait();
        }

        return $job;
    }

    /**
     * Post the job to queue.
     *
     * @param Job $job The job posted to the queue.
     * @return boolean whether operation succeed.
     */
    public function postJob(Job $job)
    {
        $job->id = uniqid('queue_', true);
        $this->channel->basic_publish(new AMQPMessage($this->serialize($job)), '', $this->queue);
        return true;
    }

    /**
     * Delay a job to the zset. This contains implemention for database
     * @param Job $job The job to delay.
     * @param $expire Expire at.
     * @return boolean whether operation succeed.
     */
    protected function delayJob(Job $job, $expire)
    {
        $job->id = uniqid('queue_', true);
        $this->channel->basic_publish(new AMQPMessage($this->serialize($job)), '', $this->queue);
        return true;
    }

    /**
     * Delete the job from the queue.
     *
     * @param Job $job The job to be deleted.
     * @return boolean whether the operation succeed.
     */
    public function deleteJob(Job $job)
    {
        return true;
    }

    /**
     * Release the job.
     *
     * @param Job $job The job to release.
     * @return boolean whether the operation succeed.
     */
    public function releaseJob(Job $job)
    {
        $this->channel->basic_publish(new AMQPMessage($this->serialize($job)), '', $this->queue);
        return true;
    }

    /**
     * Returns the total number of all queue size.
     * @return integer
     */
    public function getSize()
    {
        return $this->channel->queue_declare($this->queue, false, false, false, false);
    }

    /**
     * Purge the whole queue.
     * @return boolean
     */
    public function purge()
    {
        $this->channel->queue_purge($this->queue, true);
        return true;
    }
}
