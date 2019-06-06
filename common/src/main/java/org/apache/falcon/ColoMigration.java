package org.apache.falcon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import javax.xml.bind.JAXBException;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.Path;

public class ColoMigration {
    private static final String TMP_BASE_DIR = String.format("file://%s", new Object[]{System.getProperty("java.io.tmpdir")});

    public static void main(String[] args)
            throws Exception {
        if (args.length != 3) {
            System.out.println("Specify correct arguments");
        }
        String entitytype = args[0].trim().toLowerCase();
        String oldEntities = args[1];
        String outpath = args[2];
        changeEntities(entitytype, oldEntities, outpath);
    }

    public static void changeEntities(String entityType, String oldPath, String newPath)
            throws Exception {
        File folder = new File(oldPath);
        File[] listOfFiles = folder.listFiles();
        String stagePath = TMP_BASE_DIR + File.separator + newPath + File.separator + System.currentTimeMillis() / 1000L;
        System.out.println("Number of files: " + listOfFiles.length);
        for (File file : listOfFiles) {
            if (file.isFile()) {
                if (file.getName().contains("merlin")) {
                    continue;
                }
                System.out.println(file.getName());
                EntityType type = EntityType.getEnum(entityType);
                EntityParser<?> entityParser = EntityParserFactory.getParser(type);
                try {
                    InputStream xmlStream = new FileInputStream(file);
                    org.apache.falcon.entity.v0.process.Validity pek1Validity;
                    OutputStream out;
                    switch (type) {
                        case PROCESS:
                            Process process = (Process) entityParser.parse(xmlStream);
                            org.apache.falcon.entity.v0.process.Clusters entityClusters = process.getClusters();
                            List<org.apache.falcon.entity.v0.process.Cluster> clusters = entityClusters.getClusters();


                            List<org.apache.falcon.entity.v0.process.Cluster> processClusterToRemove = new ArrayList<>();
                            for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
                                if (cluster.getName().equals("maa1-beryl")) {
                                    processClusterToRemove.add(cluster);
                                }
                            }
                            clusters.removeAll(processClusterToRemove);

//                            org.apache.falcon.entity.v0.process.Cluster clusterToadd = new org.apache.falcon.entity.v0.process.Cluster();
//
//
//                            boolean filterAmsP =false;
//
//                            // filter on start date for processes
                            boolean filter = false;
                            List<String> processClusterNames = new ArrayList<>();
                            for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
                                Date clusterDate = cluster.getValidity().getEnd();
                                processClusterNames.add(cluster.getName());
                                if (clusterDate.getTime() > System.currentTimeMillis()) {
                                    filter = true;
                                }
//                                if(filterAmsP == false && cluster.getName().equals("ams1-azurite")){
//                                    clusterToadd.setName("maa1-beryl");
//                                    clusterToadd.setSla(cluster.getSla());
//                                    org.apache.falcon.entity.v0.process.Validity validity = new org.apache.falcon.
//                                            entity.v0.process.Validity();
//                                    validity.setStart(cluster.getValidity().getStart());
//                                    validity.setEnd(cluster.getValidity().getEnd());
//                                    clusterToadd.setValidity(validity);
//                                    clusterToadd.setVersion(cluster.getVersion());
//                                    filterAmsP = true;
//                                }
                            }
//
                                if (processClusterNames.size() != 0) {
                                    processClusterNames.add("prism");
                                }
//
//
//                            if(filterAmsP == true) {
//                                clusterToadd.setName("maa1-beryl");
//                                clusterToadd.getValidity().setStart(new Date(1542240000000L));
//                                clusters.add(clusterToadd);
//                                processClusterNames.add(clusterToadd.getName());
//                            }
//
                                if (filter) {
                                    for (String colo : processClusterNames) {
                                        File entityFile = new File(newPath + File.separator + colo + File.separator +
                                                file.getName());
                                        entityFile.getParentFile().mkdirs();
                                        if (!entityFile.createNewFile()) {
                                            System.out.println("Not able to stage the entities in the tmp path");
                                            return;
                                        }
                                        out = new FileOutputStream(entityFile);
                                        type.getMarshaller().marshal(process, out);
                                        out.close();
                                    }
                                }

                                break;


                                case FEED:
                                    Feed feed = (Feed) entityParser.parse(xmlStream);

                                    org.apache.falcon.entity.v0.feed.Clusters feedClusters = feed.getClusters();
                                    List<org.apache.falcon.entity.v0.feed.Cluster> feed_clusters = feedClusters.getClusters();


                                    List<org.apache.falcon.entity.v0.feed.Cluster> feedClusterToRemove = new ArrayList<>();
                                    for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                        if (cluster.getName().equals("maa1-beryl")) {
                                            feedClusterToRemove.add(cluster);
                                        }
                                    }
                                    feed_clusters.removeAll(feedClusterToRemove);


                                    Cluster clusterToaddFeed = new Cluster();
//
//
//                            boolean filterAms = false;
                                    List<String> feedClusterNames = new ArrayList<>();
                                    for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                        feedClusterNames.add(cluster.getName());
//                                if(cluster.getName().equals("maa1-beryl")){
//                                    Validity validity = new Validity();
//                                    validity.setStart(cluster.getValidity().getStart());
//                                    validity.setEnd(cluster.getValidity().getEnd());
//                                    clusterToaddFeed.setValidity(validity);
//                                    clusterToaddFeed.setVersion(cluster.getVersion());
//                                    clusterToaddFeed.setName(cluster.getName());
//                                    clusterToaddFeed.setDelay(cluster.getDelay());
//                                    clusterToaddFeed.setExport(cluster.getExport());
//                                    clusterToaddFeed.setImport(cluster.getImport());
//                                    clusterToaddFeed.setType(cluster.getType());
//                                    clusterToaddFeed.setTable(cluster.getTable());
//                                    clusterToaddFeed.setSla(cluster.getSla());
//                                    clusterToaddFeed.setRetention(cluster.getRetention());
//                                    clusterToaddFeed.setPartition(cluster.getPartition());
//                                    clusterToaddFeed.setLocations(cluster.getLocations());
//                                    clusterToaddFeed.setLifecycle(cluster.getLifecycle());

//                                    if(cluster.getLocations()!=null) {
//
//                                        for (Location location : cluster.getLocations().getLocations()) {
//                                            if (StringUtils.contains(location.getPath(), "ams1")) {
//                                                location.setPath(location.getPath().replace("ams1", "maa1"));
//                                                filterAms = true;
//                                            } else if (StringUtils.contains(location.getPath(), "Ams1")) {
//                                                location.setPath(location.getPath().replace("Ams1", "Maa1"));
//                                                filterAms = true;
//                                            } else if (StringUtils.contains(location.getPath(), "AMS1")) {
//                                                location.setPath(location.getPath().replace("AMS1", "MAA1"));
//                                                filterAms = true;
//                                            }
//                                        }
//                                    }
//                                }
//                            }
                                        if (feedClusterNames.size() != 0) {
                                            feedClusterNames.add("prism");
                                        }

//                            if(filterAms == true) {

//                                clusterToaddFeed.setName("maa1-beryl");
//
//                                clusterToaddFeed.getValidity().setStart(new Date(1535760000000L));
//                                feed_clusters.add(clusterToaddFeed);
//                                feedClusterNames.add(clusterToaddFeed.getName());
//                            }
                                        for (String colo : feedClusterNames) {

                                            File entityFile = new File(newPath + File.separator + colo + File.separator
                                                    + file.getName());
                                            entityFile.getParentFile().mkdirs();
                                            System.out.println("File path : " + entityFile.getAbsolutePath());
                                            if (!entityFile.createNewFile()) {
                                                System.out.println("Not able to stage the entities in the tmp path");
                                                return;
                                            }
                                            out = new FileOutputStream(entityFile);
                                            type.getMarshaller().marshal(feed, out);
                                            out.close();
                                        }
                                    }
                            }
                    } catch(FileNotFoundException e){
                        System.out.println(e.toString());
                    } catch(FalconException e){
                        System.out.println(e.toString());
                    } catch(IOException e){
                        e.printStackTrace();
                    } catch(Exception e){
                        System.out.println(e.toString());
                    }
                }
            }
        }
    }
