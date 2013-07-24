package com.cloudera.sa.fairscheduler.plus;

import java.io.IOException;
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
public class PoolManagerPlus extends PoolManager {

	static final Log LOG = LogFactory.getLog(PoolManagerPlus.class);
	
	Pattern commaSplit = Pattern.compile(",");
	
	public PoolManagerPlus(FairScheduler scheduler) {
		super(scheduler);
		LOG.info("PoolManagerPlus 0.2 was successfully constructed");
	}

	public String getPoolName(JobInProgress job) {

		//Get all pool names
		Set<String> poolSet = this.getDeclaredPools();
		String superPoolName = super.getPoolName(job);
		
		String resultingPoolName = superPoolName;
		
		if (poolSet.contains(superPoolName) == false) {
			//Get the user from the job config
			String user = job.getUser();
						
			//Get the user groups from the local OS
			try {
				user = UserGroupInformation.createProxyUser(user,
						UserGroupInformation.getLoginUser()).getUserName();
				
				if (poolSet.contains(user) == true) {
					resultingPoolName = user;
				} else {
					String[] groups = UserGroupInformation.createProxyUser(user,
							UserGroupInformation.getLoginUser()).getGroupNames();
					for (String group: groups) {
						if (poolSet.contains(group) == true) {
							resultingPoolName = group;
							break;
						}
					}
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		return resultingPoolName;
		
	}

}
