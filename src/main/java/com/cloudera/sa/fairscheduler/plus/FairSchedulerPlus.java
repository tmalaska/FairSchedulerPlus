package com.cloudera.sa.fairscheduler.plus;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FairScheduler;
import org.apache.hadoop.mapred.PoolManager;

/**
 * Extending the original FairScheduler. The only change is to override the pool
 * manager.  Everything else is left untouched.
 * 
 * @author ted.malaska
 * 
 */
public class FairSchedulerPlus extends FairScheduler {
	static final Log LOG = LogFactory.getLog(FairSchedulerPlus.class);

	@Override
	public void start() {
		super.start();
		PoolManager tempPoolMgr = poolMgr;
		try {
			poolMgr = new PoolManagerPlus(this);
			poolMgr.initialize();
		} catch (Exception e) {
			LOG.error("Problem initializing PoolManagerPlus", e);
			poolMgr = tempPoolMgr;
		}
	}

}
