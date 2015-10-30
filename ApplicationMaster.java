package G3;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.javafx.tools.packager.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {
    public static void main(String[] args) throws Exception {
        final Path jarPath = new Path("hdfs:///dvm/Ytest/YARNTest1.jar");
        final String hdfsPathToPatentFiles = args[0];
        final String destHdfsFolder = args[1];
        List<String> files = DownloadUtilities.getFileListing(hdfsPathToPatentFiles);
        String command = "";
        // Initialize clients to ResourceManager and NodeManagers
        Configuration conf = new YarnConfiguration();
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        rmClient.registerApplicationMaster("", 0, "");
        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < files.size(); ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            rmClient.addContainerRequest(containerAsk);
        }

        // Obtain allocated containers and launch
        int allocatedContainers = 0;
        while (allocatedContainers < files.size()) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                
                    //Command to execute to download url to HDFS
                    ////command = "hadoop jar YARNTest1.jar G3.DownloadFileService" + " "
                    command = "java G3.DownloadFileService" + " "
                            + files.get(allocatedContainers) + " " + destHdfsFolder;
                    //command = "hdfs dfs -mkdir hdfs:///dvm/ttt5";
                    // Launch container by create ContainerLaunchContext
                    ContainerLaunchContext ctx =
                            Records.newRecord(ContainerLaunchContext.class);
                    ctx.setCommands(Collections.singletonList(command));

                // Setup jar for ApplicationMaster
                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
                appMasterJar.setType(LocalResourceType.FILE);
                appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
                appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
                appMasterJar.setSize(jarStat.getLen());
                appMasterJar.setTimestamp(jarStat.getModificationTime());
                ctx.setLocalResources(Collections.singletonMap(
                        "YARNTest1.jar", appMasterJar));

                Map<String, String> appMasterEnv = new HashMap<String, String>();
                for (String c : conf.getStrings(
                        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                    Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                            c.trim());
                }

                Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                        ApplicationConstants.Environment.PWD.$() + File.separator + "*");
                ctx.setEnvironment(appMasterEnv);

                nmClient.startContainer(container, ctx);
                ++allocatedContainers;

                }
                Thread.sleep(100);
            }

            // Now wait for containers to complete
            int completedContainers = 0;
            while (completedContainers < files.size()) {
                AllocateResponse response = rmClient.allocate(completedContainers / files.size());
                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    ++completedContainers;
                }
                Thread.sleep(100);
            }

            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(
                    FinalApplicationStatus.SUCCEEDED, "", "");
        }


    }
