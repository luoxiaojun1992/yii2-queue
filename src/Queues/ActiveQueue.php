<?php

namespace UrbanIndo\Yii2\Queue\Queues;

use UrbanIndo\Yii2\Queue\Job;
use UrbanIndo\Yii2\Queue\Queue;
use Stomp\Client;
use Stomp\SimpleStomp;
use Stomp\Transport\Bytes;

class ActiveQueue extends Queue
{
    protected $connection = null;
    protected $client = null;

    public $host = 'localhost';
    public $port = 61613;
    public $username = 'guest';
    public $password = 'guest';
    public $queue = 'yii2';

    /**
     * @return void
     */
    public function init()
    {
        parent::init();
        $this->client = new Client("tcp://{$this->host}:{$this->port}");
        $this->client->setLogin($this->username, $this->password);
        $this->connection = new SimpleStomp($this->client);
    }

    /**
     * Return next job from the queue.
     * @return Job|boolean the job or false if not found.
     */
    public function fetchJob()
    {
        $this->connection->subscribe('/queue/' . $this->queue, 'binary-sub-' . $this->queue);
        $msg = $this->connection->read();
        $this->connection->unsubscribe('/queue/' . $this->queue, 'binary-sub-' . $this->queue);
        return $this->deserialize($msg->body);
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
        $bytesMessage = new Bytes($this->serialize($job));
        $this->connection->send('/queue/' . $this->queue, $bytesMessage);
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
        $bytesMessage = new Bytes($this->serialize($job));
        $this->connection->send('/queue/' . $this->queue, $bytesMessage);
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
        $bytesMessage = new Bytes($this->serialize($job));
        $this->connection->send('/queue/' . $this->queue, $bytesMessage);
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
