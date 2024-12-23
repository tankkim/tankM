import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.StringTokenizer;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import org.apache.log4j.BasicConfigurator;
import oz.framework.api.Scheduler;
import oz.framework.api.Service;
import oz.scheduler.ScheduledTask;
import oz.scheduler.ServerInfo;
import oz.scheduler.TaskResult;
import oz.util.SortProperties;

public class OzSchedulerRun {
  private static final String DEV_SVR_IP = "172.16.54.22";
  
  private static final String TST_SVR_IP = "172.16.55.22";
  
  private static final String PRD01_SVR_IP = "172.16.52.22";
  
  private static final String PRD02_SVR_IP = "172.16.52.21";
  
  private static final String DR01_SVR_IP = "172.17.51.21";
  
  private static final String DR02_SVR_IP = "172.17.51.22";
  
  private static final String DEV_SCHD_IP = "172.16.54.25";
  
  private static final String TST_SCHD_IP = "172.16.55.25";
  
  private static final String PRD01_SCHD_IP = "172.16.53.62";
  
  private static final String PRD02_SCHD_IP = "172.16.53.52";
  
  private static final String DR01_SCHD_IP = "172.17.52.51";
  
  private static final String DR02_SCHD_IP = "172.17.52.53";
  
  private static final int SCHEDULER_PORT = 7521;
  
  private static final String Auth_ID = "ozadmin";
  
  private static final String Auth_PW = "1q1q!Q!Q";
  
  private static final String SYS_DEV = "dev";
  
  private static final String SYS_TST = "tst";
  
  private static final String SYS_PRD = "prd";
  
  private static final String SYS_DR = "dr";
  
  private static String SERVER_URL = "";
  
  private static String SCHEDULER_IP = "";
  
  private static final String NOWAIT = "N";
  
  public static void main(String[] args) throws Exception {
    String dateStr = null;
    String pattern = "yyyyMMddHHmmss";
    SimpleDateFormat sdfCurrent = new SimpleDateFormat(pattern);
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    dateStr = sdfCurrent.format(Long.valueOf(ts.getTime()));
    String sysHostNm = "";
    String sysProp = "";
    boolean onlyReq = false;
    System.out.println("Scheduler start..[" + dateStr + "]");
    System.out.println("-------------------------------------------------");
    try {
      sysHostNm = InetAddress.getLocalHost().getHostName();
      if (sysHostNm == null)
        throw new Exception("Error while getting Hostname!"); 
      sysProp = sysHostNm.substring(0, 3);
      if ("dr".equals(sysProp.substring(0, 2)))
        sysProp = "dr"; 
      System.out.println("sysProp[" + sysProp + "]");
      getServerInfo(sysProp);
      String serverUrl = SERVER_URL;
      String schedulerIp = SCHEDULER_IP;
      int schedulerPort = 7521;
      if (args.length < 1)
        throw new Exception("No Arguments input!"); 
      String inputLine = args[0];
      String inputParams = inputLine.replaceAll(";", " ;");
      System.out.println("Arguments list : " + inputLine);
      StringTokenizer tk = new StringTokenizer(inputParams, ";");
      String ozrName = tk.nextToken().replace(" ", "");
      String ozrParamValues = tk.nextToken().replace(" ", "");
      String[] ozrParamVal = null;
      int ozrParamCnt = 0;
      if (ozrParamValues.indexOf("&") > -1)
        ozrParamVal = ozrParamValues.split("&"); 
      if (ozrParamVal == null) {
        if (ozrParamValues.length() > 0) {
          ozrParamCnt = 1;
          ozrParamVal = new String[ozrParamCnt];
          ozrParamVal[0] = ozrParamValues;
        } 
      } else {
        ozrParamCnt = ozrParamVal.length;
      } 
      String odiName = "";
      if (!"".equals(ozrName))
        odiName = ozrName.substring(ozrName.lastIndexOf("/") + 1, ozrName.length() - 4); 
      String odiParamValues = tk.nextToken().replace(" ", "");
      String[] odiParamVal = null;
      if (odiParamValues.indexOf("&") > -1)
        odiParamVal = odiParamValues.split("&"); 
      int odiParamCnt = 0;
      if (odiParamVal == null) {
        if (odiParamValues.length() > 0) {
          odiParamCnt = 1;
          odiParamVal = new String[odiParamCnt];
          odiParamVal[0] = odiParamValues;
        } 
      } else {
        odiParamCnt = odiParamVal.length;
      } 
      String exportFileName = tk.nextToken().replace(" ", "");
      String exportFormat = tk.nextToken().replace(" ", "");
      String optionVal = null;
      String separator = null;
      if (tk.hasMoreTokens())
        optionVal = tk.nextToken().replace(" ", ""); 
      if (tk.hasMoreTokens())
        separator = tk.nextToken().replace(" ", ""); 
      System.out.println("exportFileName  : " + exportFileName);
      System.out.println("exportFormat    : " + exportFormat);
      System.out.println("optionVal       : " + optionVal);
      System.out.println("separator       : " + separator);
      ServerInfo serverInfo = null;
      SortProperties configMap = null;
      SortProperties exportMap = null;
      Scheduler scheduler = null;
      String taskID = "";
      BasicConfigurator.configure();
      Service service = new Service(serverUrl, "ozadmin", "1q1q!Q!Q", false, false);
      scheduler = new Scheduler(schedulerIp, schedulerPort);
      boolean serverAlive = service.ping();
      boolean schedulerAlive = scheduler.ping();
      System.out.println("serverAlive[" + serverAlive + "]");
      System.out.println("schedulerAlive[" + schedulerAlive + "]");
      if (!serverAlive) {
        if ("prd".equals(sysProp)) {
          SERVER_URL = "http://172.16.52.21:7010/oz/server";
          serverUrl = SERVER_URL;
          service = new Service(serverUrl, "ozadmin", "1q1q!Q!Q", false, false);
          serverAlive = service.ping();
        } else if ("dr".equals(sysProp.substring(0, 2))) {
          SERVER_URL = "http://172.17.51.22:7010/oz/server";
          serverUrl = SERVER_URL;
          service = new Service(serverUrl, "ozadmin", "1q1q!Q!Q", false, false);
          serverAlive = service.ping();
        } else {
          throw new Exception("OZ Server is dead [" + serverUrl + "]");
        } 
        System.out.println("serverAlive 02[" + serverAlive + "]");
      } 
      if (!schedulerAlive) {
        if ("prd".equals(sysProp)) {
          SCHEDULER_IP = "172.16.53.52";
          schedulerIp = SCHEDULER_IP;
          scheduler = new Scheduler(schedulerIp, schedulerPort);
          schedulerAlive = scheduler.ping();
        } else if ("dr".equals(sysProp.substring(0, 2))) {
          SCHEDULER_IP = "172.17.52.51";
          schedulerIp = SCHEDULER_IP;
          scheduler = new Scheduler(schedulerIp, schedulerPort);
          schedulerAlive = scheduler.ping();
        } else {
          throw new Exception("OZ Scheduler is dead [" + schedulerIp + ":" + schedulerPort + "]");
        } 
        System.out.println("schedulerAlive 02[" + schedulerAlive + "]");
      } 
      if (serverAlive && schedulerAlive) {
        serverInfo = new ServerInfo();
        serverInfo.setID("ozadmin");
        serverInfo.setPWD("1q1q!Q!Q");
        serverInfo.setIsDaemon(false);
        serverInfo.setURL(serverUrl);
        configMap = new SortProperties();
        configMap.setProperty("task_type", "viewerTag");
        configMap.setProperty("launch_type", "immediately");
        configMap.setProperty("cfg.type", "new");
        exportMap = new SortProperties();
        exportMap.setProperty("connection.servlet", serverUrl);
        exportMap.setProperty("connection.fetchtype", "CONCURRENT");
        exportMap.setProperty("connection.pageque", "100");
        exportMap.setProperty("connection.serverdmtype", "MEMORY");
        exportMap.setProperty("connection.reportname", ozrName);
        exportMap.setProperty("connection.pcount", Integer.toString(ozrParamCnt));
        int i;
        for (i = 0; i < ozrParamCnt; i++)
          exportMap.setProperty("connection.args" + Integer.toString(i + 1), ozrParamVal[i]); 
        exportMap.setProperty("odi.odinames", odiName);
        exportMap.setProperty("odi." + odiName + ".pcount", Integer.toString(odiParamCnt));
        for (i = 0; i < odiParamCnt; i++)
          exportMap.setProperty("odi." + odiName + ".args" + Integer.toString(i + 1), odiParamVal[i]); 
        exportMap.setProperty("viewer.mode", "preview");
        exportMap.setProperty("export.mode", "silent");
        exportMap.setProperty("export.confirmsave", "false");
        exportMap.setProperty("export.format", exportFormat);
        if (exportFormat.equals("xls")) {
          exportMap.setProperty("excel.filename", String.valueOf(exportFileName) + "." + exportFormat);
        } else if (exportFormat.equals("txt")) {
          exportMap.setProperty("text.filename", String.valueOf(exportFileName) + "." + exportFormat);
        } else if (exportFormat.equals("hwp")) {
          exportMap.setProperty("hwp.filename", exportFileName);
        } else if (exportFormat.equals("hml")) {
          exportMap.setProperty("han97.filename", exportFileName);
        } else if (exportFormat.equals("hdm")) {
          exportMap.setProperty("hdm.extension", "xls");
          exportMap.setProperty("hdm.filename", exportFileName);
          exportMap.setProperty("hdm.save_description_as_title", "true");
        } else if (exportFormat.equals("csv")) {
          exportMap.setProperty("csv.filename", String.valueOf(exportFileName) + "." + exportFormat);
          if (separator != null)
            exportMap.setProperty("csv.separator", separator); 
        } else {
          exportMap.setProperty(String.valueOf(exportFormat) + ".filename", String.valueOf(exportFileName) + "." + exportFormat);
        } 
        taskID = scheduler.createTask(serverInfo, configMap, exportMap);
        ScheduledTask[] taskList = scheduler.getTask(serverInfo);
        System.out.println("=============================================================");
        byte b;
        int j;
        ScheduledTask[] arrayOfScheduledTask1;
        for (j = (arrayOfScheduledTask1 = taskList).length, b = 0; b < j; ) {
          ScheduledTask task = arrayOfScheduledTask1[b];
          if (taskID.equals(task.taskID))
            showTask(task); 
          b++;
        } 
        System.out.println("=============================================================");
        boolean isComplete = false;
        if (!"N".equals(optionVal)) {
          while (!isComplete) {
            ScheduledTask[] arrayOfScheduledTask = scheduler.getTask(serverInfo);
            if (arrayOfScheduledTask.length == 0)
              isComplete = true; 
            Thread.sleep(5000L);
            boolean statusChk = true;
            System.out.println("taskList.length[" + arrayOfScheduledTask.length + "]");
            for (int k = 0; k < arrayOfScheduledTask.length; k++) {
              System.out.println("taskList[" + k + "].taskID[" + (arrayOfScheduledTask[k]).taskID + "]");
              System.out.println("taskList[" + k + "].status[" + (arrayOfScheduledTask[k]).status + "]");
              if ((arrayOfScheduledTask[k]).taskID.equals(taskID) && 
                (arrayOfScheduledTask[k]).status == 'R')
                statusChk = false; 
            } 
            System.out.println("statusChk[" + statusChk + "]");
            if (statusChk)
              isComplete = true; 
            System.out.println("isComplete[" + isComplete + "]");
          } 
          System.out.println("=============================================================");
          System.out.println("=============================================================");
          if (!"N".equals(optionVal) && optionVal != null && !"".equals(optionVal))
            compressZip("/batch/report" + exportFileName, exportFormat, optionVal); 
        } else {
          onlyReq = true;
        } 
      } else {
        if (!serverAlive)
          throw new Exception("OZ Server is dead [" + serverUrl + "]"); 
        if (!schedulerAlive)
          throw new Exception("OZ Scheduler is dead [" + schedulerIp + ":" + schedulerPort + "]"); 
      } 
    } catch (Exception e) {
      String errMsg = "";
      errMsg = e.getMessage();
      if (errMsg == null)
        errMsg = "Syntax Error"; 
      System.out.println("Batch Error : " + errMsg);
      e.printStackTrace();
      ts = new Timestamp(System.currentTimeMillis());
      dateStr = sdfCurrent.format(Long.valueOf(ts.getTime()));
      System.out.println("-------------------------------------------------");
      System.out.println("Scheduler ended..[" + dateStr + "]\n");
      System.exit(1);
    } 
    ts = new Timestamp(System.currentTimeMillis());
    dateStr = sdfCurrent.format(Long.valueOf(ts.getTime()));
    System.out.println("-------------------------------------------------");
    if (!onlyReq) {
      System.out.println("Scheduler ended..[" + dateStr + "]\n");
    } else {
      System.out.println("Scheduler request ended..[" + dateStr + "]\n");
    } 
    System.exit(0);
  }
  
  private static void getServerInfo(String sysProp) {
    String subUrl = ":7010/oz/server";
    String str1;
    switch ((str1 = sysProp).hashCode()) {
      case 3214:
        if (!str1.equals("dr"))
          break; 
        SERVER_URL = "http://172.17.51.21" + subUrl;
        SCHEDULER_IP = "172.17.52.53";
        break;
      case 99349:
        if (!str1.equals("dev"))
          break; 
        SERVER_URL = "http://172.16.54.22" + subUrl;
        SCHEDULER_IP = "172.16.54.25";
        break;
      case 111266:
        if (!str1.equals("prd"))
          break; 
        SERVER_URL = "http://172.16.52.22" + subUrl;
        SCHEDULER_IP = "172.16.53.62";
        break;
      case 115157:
        if (!str1.equals("tst"))
          break; 
        SERVER_URL = "http://172.16.55.22" + subUrl;
        SCHEDULER_IP = "172.16.55.25";
        break;
    } 
    System.out.println("SERVER_URL[" + SERVER_URL + "]");
    System.out.println("SCHEDULER_IP[" + SCHEDULER_IP + "]");
  }
  
  private static void showTask(ScheduledTask t) {
    System.out.println("TASK ID : " + t.taskID);
    System.out.println("TASK Name : " + t.taskName);
    System.out.println("TASK Group Name : " + t.taskGroupName);
    System.out.println("Report Name : " + t.reportName);
    System.out.println("Type : " + t.schedulingTypeStr);
    System.out.println("Finish Execute Time : " + t.lastRunTimeStr);
    System.out.println("Next Execute Time : " + t.nextRunTimeStr);
    System.out.println("Status : " + t.status);
  }
  
  private static void showRsltTask(TaskResult t) throws UnsupportedEncodingException {
    System.out.println("Task ID :" + t.taskID);
    System.out.println("Success Flag : " + t.isSuccessful);
    System.out.println("Form File Name : " + t.formFileName);
    System.out.println("Completed Time :" + t.completedTime);
    System.out.println("Export path : " + t.exportFileList);
    if (!"".equals(t.errorMsg)) {
      System.out.println("ErrorMsg : " + new String(t.errorMsg.getBytes("8859_1"), "euc_kr"));
      System.out.println("=============================================================");
      System.exit(1);
    } 
  }
  
  private static void compressZip(String prmFileNm, String prmFileType, String prmPwd) {
    try {
      ZipFile zFile = new ZipFile(String.valueOf(prmFileNm) + "." + prmFileType + ".zip");
      ZipParameters zParam = new ZipParameters();
      ArrayList<File> filesToAdd = new ArrayList<>();
      System.out.println("prmFileNm    : " + prmFileNm);
      System.out.println("prmPwd       : " + prmPwd);
      filesToAdd.add(new File(String.valueOf(prmFileNm) + "." + prmFileType));
      zParam.setCompressionMethod(8);
      zParam.setCompressionLevel(5);
      zParam.setEncryptFiles(true);
      zParam.setEncryptionMethod(0);
      zParam.setPassword(prmPwd);
      zFile.addFiles(filesToAdd, zParam);
    } catch (ZipException e) {
      e.printStackTrace();
      System.exit(1);
    } 
  }
}
