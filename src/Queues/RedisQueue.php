<?php
/**
 * DbQueue class file.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 * @since 2016.01.16
 */

namespace UrbanIndo\Yii2\Queue\Queues;

use yii\redis\Connection;
use UrbanIndo\Yii2\Queue\Job;

/**
 * RedisQueue provides Redis storing for Queue.
 *
 * This uses `yiisoft/yii2-redis` extension that doesn't shipped in the default
 * composer dependency. To use this you have to manually add `yiisoft/yii2-redis`
 * in the `composer.json`.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 * @since 2016.01.16
 */
class RedisQueue extends \UrbanIndo\Yii2\Queue\Queue
{
    /**
     * Stores the redis connection.
     * @var string|array|Connection
     */
    public $db = 'redis';
    
    /**
     * The name of the key to store the queue.
     * @var string
     */
    public $key = 'queue';

    /**
     * The name of the key to store delayed queues.
     * @var string
     */
    public $delayKey = 'queue:delayed';
    
    /**
     * @return void
     */
    public function init()
    {
        parent::init();
        $this->db = \yii\di\Instance::ensure($this->db, Connection::className());
    }

    /**
     * Delete the job.
     *
     * @param Job $job The job to delete.
     * @return boolean whether the operation succeed.
     */
    public function deleteJob(Job $job)
    {
        return true;
    }

    /**
     * Return next job from the queue.
     * @return Job|boolean the job or false if not found.
     */
    protected function fetchJob()
    {
        // Migrating Delayed Queues
        $delayed_queues = $this->db->zrange($this->delayKey, 0, -1);
        foreach ($delayed_queues as $delayed_queue) {
            if ($delayed_queue) {
                $data = \yii\helpers\Json::decode($delayed_queue);
                if ($data['expire'] <= date('Y-m-d H:i:s')) {
                    $this->db->watch($this->delayKey . '@' . $data['id']);
                    $this->db->multi();
                    $this->db->zrem($this->delayKey, $delayed_queue);
                    $job = $this->deserialize($data['data']);
                    $job->id = $data['id'];
                    $job->header['serialized'] = $data['data'];
                    $this->releaseJob($job);
                    try {
                        $this->db->exec();
                    } catch (\Exception $e) {
                        $this->db->discard();
                    }
                }
            }
        }

        $json = $this->db->lpop($this->key);
        if ($json == false) {
            return false;
        }
        $data = \yii\helpers\Json::decode($json);

        $job = $this->deserialize($data['data']);
        $job->id = $data['id'];
        $job->header['serialized'] = $data['data'];
        return $job;
    }

    /**
     * Post new job to the queue.  This contains implementation for database.
     *
     * @param Job $job The job to post.
     * @return boolean whether operation succeed.
     */
    protected function postJob(Job $job)
    {
        return $this->db->rpush($this->key, \yii\helpers\Json::encode([
            'id' => uniqid('queue_', true),
            'data' => $this->serialize($job),
        ]));
    }

    /**
     * Delay a job to the zset. This contains implemention for database
     * @param Job $job The job to delay.
     * @param $expire Expire at.
     * @return boolean whether operation succeed.
     */
    protected function delayJob(Job $job, $expire)
    {
        return $this->db->zadd($this->delayKey, 0, \yii\helpers\Json::encode([
            'id' => $job->id ? : uniqid('queue_', true),
            'data' => empty($job->header['serialized']) ? $this->serialize($job) : $job->header['serialized'],
            'expire' => $expire,
        ]));
    }

    /**
     * Put back job to the queue.
     *
     * @param Job $job The job to restore.
     * @return boolean whether the operation succeed.
     */
    protected function releaseJob(Job $job)
    {
        return $this->db->rpush($this->key, \yii\helpers\Json::encode([
            'id' => $job->id,
            'data' => $job->header['serialized'],
        ]));
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
