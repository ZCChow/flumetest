package com.chow.flume;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeHeader implements HTTPSourceHandler {

	 private static final Logger LOG = LoggerFactory.getLogger(FlumeHeader.class);
	 private final String HEADER_Tag = "header_tag"; 
	  public void configure(Context context) {}
	  
	  public List<Event> getEvents(HttpServletRequest request)
	    throws HTTPBadRequestException, Exception
	  {
	    request.setCharacterEncoding("UTF-8");
	    

	    String outputDir = "/usr/local/src/temp";
	     ArrayList<Event> event_list = new ArrayList(6);
	    TarInputStream tarIn = null;
	    Map<String, String> eventHeaders = new HashMap<String, String>();
	    try
	    {
	      tarIn = new TarInputStream(new GZIPInputStream(new BufferedInputStream(request.getInputStream())));
	      createDirectory(outputDir, null);
	      LOG.info("Directory create success");
	      TarEntry entry = null;
	      while ((entry = tarIn.getNextEntry()) != null)
	      {
	        LOG.info("input first while");
	        if (entry.isDirectory())
	        {
	          entry.getName();
	          
	          createDirectory(outputDir, entry.getName());
	        }
	        else
	        {
	          LOG.info("start write file");
	          File tmpFile = new File(outputDir + "/" + entry.getName());
	          
	          createDirectory(tmpFile.getParent() + "/", null);
	         
	          OutputStream out = null;
	          out = new FileOutputStream(tmpFile);
	          int length = 0;
	          byte[] b = new byte[4096];
	          while ((length = tarIn.read(b)) != -1)
	          {
	            LOG.info("input second while");
	            out.write(b, 0, length);
	          }
	          if (out != null) {
	            out.close();
	          }
	          LOG.info("write file success");
	        }
	        LOG.info("start read file");
	        File files = new File(outputDir + "/" + entry.getName());
	        BufferedReader reader = null;
	        reader = new BufferedReader(new FileReader(files));
	        String tempString = null;
	        while ((tempString = reader.readLine()) != null)
	        {
	          LOG.info("file content = " + tempString);
	          if ((tempString != null) || (!tempString.equals("")))
	          {
	            LOG.info("event_list add the event begin : ");
	            eventHeaders.put(HEADER_Tag, entry.getName().substring(0, entry.getName().length()-14));
	          //  boolean aa = event_list.add(EventBuilder.withBody(tempString.getBytes(request.getCharacterEncoding())),eventHeaders);
	         boolean aa = event_list.add(EventBuilder.withBody(tempString.getBytes(request.getCharacterEncoding()), eventHeaders));
	         LOG.info("event_list add the event  end  : " + aa);
	          }
	        }
	        if (reader != null) {
	          reader.close();
	        }
	        LOG.info("read file end and start delete file");
	        boolean b = deleteFile(outputDir + "/" + entry.getName());
	        LOG.info("delete file" + b);
	      }
	      return event_list;
	    }
	    catch (IOException ex)
	    {
	      throw new IOException("解压归档文件出现异常", ex);
	    }
	    finally
	    {
	      try
	      {
	        if (tarIn != null) {
	          tarIn.close();
	        }
	      }
	      catch (IOException ex)
	      {
	        throw new IOException("关闭tarFile出现异常", ex);
	      }
	    }
	  }
	  
	  public static void createDirectory(String outputDir, String subDir)
	  {
	    File file = new File(outputDir);
	    if ((subDir != null) && (!subDir.trim().equals(""))) {
	      file = new File(outputDir + "/" + subDir);
	    }
	    if (!file.exists())
	    {
	      if (!file.getParentFile().exists()) {
	        file.getParentFile().mkdirs();
	      }
	      file.mkdirs();
	    }
	  }
	  
	  public boolean deleteFile(String sPath)
	  {
	    boolean flag = false;
	    File file = new File(sPath);
	    if ((file.isFile()) && (file.exists()))
	    {
	      file.delete();
	      flag = true;
	    }
	    return flag;
	  }
	  
	  public boolean deleteDirectory(String sPath)
	  {
	    if (!sPath.endsWith(File.separator)) {
	      sPath = sPath + File.separator;
	    }
	    File dirFile = new File(sPath);
	    if ((!dirFile.exists()) || (!dirFile.isDirectory())) {
	      return false;
	    }
	    boolean flag = true;
	    
	    File[] files = dirFile.listFiles();
	    for (int i = 0; i < files.length; i++) {
	      if (files[i].isFile())
	      {
	        flag = deleteFile(files[i].getAbsolutePath());
	        if (!flag) {
	          break;
	        }
	      }
	      else
	      {
	        flag = deleteDirectory(files[i].getAbsolutePath());
	        if (!flag) {
	          break;
	        }
	      }
	    }
	    if (!flag) {
	      return false;
	    }
	    if (dirFile.delete()) {
	      return true;
	    }
	    return false;
	  }

}
