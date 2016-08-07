package com.big.data.plan.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * hdfs test
 */
public class HDFS_File {

    private FileSystem hdfs;

    private Configuration conf;

    private static Logger logger = Logger.getLogger(HDFS_File.class);

    public HDFS_File(Configuration conf) {
        this.conf = conf;
        try {
            this.hdfs = FileSystem.get(conf);
            logger.info("==========>get FileSystem");
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        HDFS_File file = new HDFS_File(conf);
//        file.getFiles("/");
        boolean rtn = file.checkFileExist("hdfs://localhost:9000/test");
        logger.info("=========>rtn:"+rtn);
    }

    /**
     * read the file from HDFS
     * @param FileName
     * @return
     */
    public boolean readFile(String FileName) {
        try {
            FSDataInputStream dis = hdfs.open(new Path(FileName));
            IOUtils.copyBytes(dis, System.out, 4096, false);
            dis.close();
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * copy the file from HDFS to local
     * @param srcFile
     * @param dstFile
     * @return
     */
    public boolean getFile(String srcFile, String dstFile) {
        try {
            Path srcPath = new Path(srcFile);
            Path dstPath = new Path(dstFile);
            hdfs.copyToLocalFile(true, srcPath, dstPath);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * mkdir
     * @param dir
     * @return
     */
    public boolean mKDir(String dir) {
        try {
            Path srcPath = new Path(dir);
            hdfs.mkdirs(srcPath);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * copy the local file to HDFS
     * @param srcFile
     * @param dstFile
     * @return
     */
    public boolean putFile(String srcFile, String dstFile) {
        try {
            Path srcPath = new Path(srcFile);
            Path dstPath = new Path(dstFile);
            hdfs.copyFromLocalFile(srcPath, dstPath);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * create the new file
     * @param FileName
     * @return
     */
    public FSDataOutputStream createFile(String FileName) {
        try {
            Path path = new Path(FileName);
            FSDataOutputStream outputStream = hdfs.create(path);
            return outputStream;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /**
     * rename the file name
     * @param srcName
     * @param dstName
     * @return
     */
    public boolean reNameFile(String srcName, String dstName) {
        try {
            Path fromPath = new Path(srcName);
            Path toPath = new Path(dstName);
            boolean isRenamed = hdfs.rename(fromPath, toPath);
            return isRenamed;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    /**
     * del
     * @param FileName
     * @param type:true, delete the directory,false, delete the file
     * @return
     */
    public boolean delFile(String FileName, boolean type) {
        try {
            Path path = new Path(FileName);
            boolean isDeleted = hdfs.delete(path, type);
            return isDeleted;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    /**
     * Get HDFS file last modification time
     * @param FileName
     * @return
     */
    public long getFileModTime(String FileName) {
        try {
            Path path = new Path(FileName);
            FileStatus fileStatus = hdfs.getFileStatus(path);
            long modificationTime = fileStatus.getModificationTime();
            return modificationTime;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return 0;
    }

    /**
     * check if a file exists in HDFS
     * @param FileName
     * @return
     */
    public boolean checkFileExist(String FileName) {
        try {
            Path path = new Path(FileName);
            boolean isExists = hdfs.exists(path);
            return isExists;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    /**
     * Get the locations of a file in the HDFS cluster
     * @param FileName
     * @return
     */
    public List<String[]> getFileLocations(String FileName) {
        try {
            List<String[]> list = new ArrayList<String[]>();
            Path path = new Path(FileName);
            FileStatus fileStatus = hdfs.getFileStatus(path);

            BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus, 0,
                fileStatus.getLen());

            int blkCount = blkLocations.length;
            for (int i = 0; i < blkCount; i++) {
                String[] hosts = blkLocations[i].getHosts();
                list.add(hosts);
            }
            return list;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /**
     * get all node name
     * @return
     */
    public String[] getAllNodeName() {
        try {
            DistributedFileSystem dbhdfs = (DistributedFileSystem) hdfs;
            DatanodeInfo[] dataNodeStats = dbhdfs.getDataNodeStats();
            String[] names = new String[dataNodeStats.length];
            for (int i = 0; i < dataNodeStats.length; i++) {
                names[i] = dataNodeStats[i].getHostName();
            }
            return names;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /**
     * get files
     * @param src
     * @throws IOException
     */
    public void getFiles(String src) throws IOException {
        Path path = new Path(src);
        FileStatus[] fileStatus = hdfs.listStatus(path);
        for (int i = 0; i < fileStatus.length; i++) {
            if (fileStatus[i].isDir()) {
                getFiles(fileStatus[i].getPath().toString());
            } else {
                logger.info(fileStatus[i].getPath().toString());
            }
        }
    }

    /**
     * append
     * @param src
     * @return
     */
    public FSDataOutputStream append(String src) {
        Path path = new Path(src);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = hdfs.append(path);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return fsDataOutputStream;
    }
}