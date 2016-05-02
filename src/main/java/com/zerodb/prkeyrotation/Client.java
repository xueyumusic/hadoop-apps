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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


public class Client {
	Configuration conf = new YarnConfiguration();
	
	public void run(String[] args) throws Exception {
		YarnConfiguration conf = new YarnConfiguration();
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();
		
		final Path jarPath = new Path(args[0]);
		
		YarnClientApplication app = yarnClient.createApplication();
		
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		
		amContainer.setCommands(
				Collections.singletonList(
				//"/usr/bin/java"+
				Environment.JAVA_HOME.$$() + "/bin/java" + 
				" -Xmx1024M"+
				" com.zerodb.prkeyrotation.ApplicationMaster"+
				" " + jarPath + 
				" 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
				" 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
		
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		setupAppMasterJar(jarPath, appMasterJar);
		amContainer.setLocalResources(Collections.singletonMap("prkeyrotation.jar", appMasterJar));
		
		Map<String, String> appMasterEnv = new HashMap<String, String>();
		setupAppMasterEnv(appMasterEnv);
		amContainer.setEnvironment(appMasterEnv);
		
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(1024);
		capability.setVirtualCores(1);
		
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appContext.setApplicationName("prkeyrotation");
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		appContext.setQueue("default");
		
		ApplicationId appId = appContext.getApplicationId();
		System.out.println("Submitting application "+appId);
		yarnClient.submitApplication(appContext);
		
		ApplicationReport appReport = yarnClient.getApplicationReport(appId);
		YarnApplicationState appState = appReport.getYarnApplicationState();
		while (appState != YarnApplicationState.FINISHED &&
				appState != YarnApplicationState.KILLED &&
				appState != YarnApplicationState.FAILED) {
			Thread.sleep(100);
			appReport = yarnClient.getApplicationReport(appId);
			appState = appReport.getYarnApplicationState();
		}
		
		System.out.println("Application " + appId + " finished with " +
								" state " + appState +
								" at " + appReport.getFinishTime());
		
		
	}
	
	private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
		FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
		appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
		appMasterJar.setSize(jarStat.getLen());
		appMasterJar.setTimestamp(jarStat.getModificationTime());
		appMasterJar.setType(LocalResourceType.FILE);
		appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
	}
	
	private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
		for (String c: conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH, 
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
		}
		Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
				Environment.PWD.$() + File.separator +"*");
	}
	
	public static void main(String[] args) throws Exception {
		Client c = new Client();
		c.run(args);
	}
}
