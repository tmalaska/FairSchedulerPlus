package com.cloudera.sa.fairscheduler.plus;

import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FairScheduler;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Pool;
import org.apache.hadoop.mapred.PoolManager;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This is a extension of PoolManager.  This will use the 
 * user name then group names then config value to select the correct 
 * pool.
 * 
 * @author ted.malaska
 *
 */
public class CopyOfPoolManagerPlus extends PoolManager {

	static final Log LOG = LogFactory.getLog(CopyOfPoolManagerPlus.class);
	
	Pattern commaSplit = Pattern.compile(",");
	
	public CopyOfPoolManagerPlus(FairScheduler scheduler) {
		super(scheduler);
		LOG.info("PoolManagerPlus was successfully constructed");
	}
	
	/**
	 * Get a pool by name, creating it if necessary
	 */
	@Override
	public synchronized Pool getPool(String nameCommaSeperatedList) {
		
		//split the inputed value by commas.  To separate all the pool names
		String[] poolNames = commaSplit.split(nameCommaSeperatedList);
		
		//Get all pool names
		Set<String> poolSet = this.getDeclaredPools();
		
		//Iterate through all the pool names and see is a matching pool exist
		for (String poolName: poolNames) {
			if (poolSet.contains(poolName)) {
				//We have found a matching queue
				LOG.info("Selected '" + poolName + "' pool.");
				return super.getPool(poolName);
			}
		}
		//We did not find a matching queue
		LOG.info("Unable to find pool calling normal FairScheduler to reach default option.");
		return super.getPool(poolNames[0]);
	}

	/**
	 * Get the pool name for a JobInProgress from its configuration. This uses
	 * the value of mapred.fairscheduler.pool if specified, otherwise the value
	 * of the property named in mapred.fairscheduler.poolnameproperty if that is
	 * specified. Otherwise if neither is specified it uses the "user.name"
	 * property in the jobconf by default.
	 */
	public String getPoolName(JobInProgress job) {

		StringBuilder strBuilder = new StringBuilder();
		try {
			//Get the user from the job config
			String user = job.getUser();
						
			//Get the user groups from the local OS
			user = UserGroupInformation.createProxyUser(user,
					UserGroupInformation.getLoginUser()).getUserName();
			String[] groups = UserGroupInformation.createProxyUser(user,
					UserGroupInformation.getLoginUser()).getGroupNames();
			
			//Put all the values into a comma separated string
			strBuilder.append(user + ",");
			for (String group : groups) {
				strBuilder.append(group + ",");
			}
			
		} catch (Exception e) {
			LOG.error("Problem while getting multipart pool name", e);
		}
		//Add the default value from parent fairscheduler as the last option
		return strBuilder.toString() + super.getPoolName(job);
		
	}

}
