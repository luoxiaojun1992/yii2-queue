<?php

namespace UrbanIndo\Yii2\Queue\Queues;

use UrbanIndo\Yii2\Queue\Job;
use UrbanIndo\Yii2\Queue\Queue;

class KafkaQueue extends Queue
{
    protected $producer;
    protected $consumer;

    public $host = '127.0.0.1';
    public $port = 2181;
    public $timeout = 3000;
    public $group = 'yii2group';
    public $topic = 'yii2';
    public $partition = 0;

    /**
     * @return void
     */
    public function init()
    {
        parent::init();

        $this->producer = \Kafka\Produce::getInstance(implode(':', [$this->host, $this->port]), $this->timeout);
        $this->producer->setRequireAck(-1);

        $this->consumer = \Kafka\Consumer::getInstance(implode(':', [$this->host, $this->port]));
        $this->consumer->setGroup($this->group);
        $this->consumer->setPartition($this->topic, $this->partition);
    }

    /**
     * Return next job from the queue.
     * @return Job|boolean the job or false if not found.
     */
    public function fetchJob()
    {
        $result = $this->consumer->fetch();
        foreach ($result as $topicName => $topic) {
            foreach ($topic as $partId => $partition) {
                foreach ($partition as $message) {
                    return $this->deserialize((string)$message);
                }
            }
        }

        return false;
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
        $this->producer->setMessages($this->topic, $this->partition, [$this->serialize($job)]);
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
        $this->producer->setMessages($this->topic, $this->partition, [$this->serialize($job)]);
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
        $this->producer->setMessages($this->topic, $this->partition, [$this->serialize($job)]);
        return true;
    }

    /**
     * Returns the total number of all queue size.
     * @return integer
     */
    public function getSize()
    {
        return 0;
    }

    /**
     * Purge the whole queue.
     * @return boolean
     */
    public function purge()
    {
        return true;
    }
}
