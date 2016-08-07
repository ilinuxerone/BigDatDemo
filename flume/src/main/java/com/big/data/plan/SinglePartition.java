package com.big.data.plan;

/**
 * Kafka Simple Partitioner.
 * User: beyondj2ee
 * Date: 13. 9. 4
 * Time: PM 5:39
 */

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Single partition.
 */
public class SinglePartition implements Partitioner {
    /**
     * The constant LOGGER.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SinglePartition.class);

    /**
     * Instantiates a new Single partition.
     *
     * @param props the props
     */
    public SinglePartition(VerifiableProperties props) {
    }

    /**
     * choose only one partition.
     *
     * @param key partition key
     * @param numberOfPartions number of partitions
     * @return the int
     */
    @Override
    public int partition(Object key, int numberOfPartions) {
        return 0;
    }
}
