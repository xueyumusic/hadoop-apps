package com.zerodb.prkeyrotation;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


public class ApplicationMaster {
	Configuration conf = new YarnConfiguration();
	
	private void setupAppJar(Path jarPath, LocalResource appMasterJar) throws IOException {
		FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
		appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
		appMasterJar.setSize(jarStat.getLen());
		appMasterJar.setTimestamp(jarStat.getModificationTime());
		appMasterJar.setType(LocalResourceType.FILE);
		appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
	}
	
	private void setupAppEnv(Map<String, String> appMasterEnv) {
		for (String c: conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH, 
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
		}
		Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
				Environment.PWD.$() + File.separator +"*");
	}
	
	public void run(String[] args) throws Exception {
		int n = 1;
		final Path jarPath = new Path(args[0]);
		
		Configuration conf = new YarnConfiguration();
		
		AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
		rmClient.init(conf);
		rmClient.start();
		
		NMClient nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();
		
		System.out.println("registerApplicationMaster 0");
		rmClient.registerApplicationMaster("", 0, "");
		System.out.println("registerApplicationMaster 1");
		
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);
		
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(256);
		capability.setVirtualCores(1);
		
		for (int i = 0; i < n; i++) {
			ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
			System.out.println("Making res-req " + i);
			rmClient.addContainerRequest(containerAsk);
		}
		
		int responseId = 0;
		int completedContainers = 0;
		while (completedContainers < n) {
			AllocateResponse response = rmClient.allocate(responseId++);
			for (Container container : response.getAllocatedContainers()) {
				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				ctx.setCommands(
						Collections.singletonList(
						//"/usr/bin/java"+
						Environment.JAVA_HOME.$$() + "/bin/java" + 
						" -Xmx1024M"+
						" com.zerodb.prkeyrotation.TestKeyRotation"+
						" 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
						" 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
				
				LocalResource jarResource = Records.newRecord(LocalResource.class);
				setupAppJar(jarPath, jarResource);
				ctx.setLocalResources(Collections.singletonMap("prkeyrotation.jar", jarResource));
				
				Map<String,String> appEnv = new HashMap<String, String>();
				setupAppEnv(appEnv);
				ctx.setEnvironment(appEnv);
				
				
				System.out.println("Launching container " + container.getId());
				nmClient.startContainer(container, ctx);
			}
			
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				completedContainers++;
				System.out.println("Completed container " + status.getContainerId());
			}
			Thread.sleep(100);
		}
		
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
		
	}
	
	public static void main(String[] args) throws Exception {
		ApplicationMaster appmaster = new ApplicationMaster();
		appmaster.run(args);
	}
}
