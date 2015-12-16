package sample;

import net.IOControl;
import net.Session;

import org.ini4j.InvalidFileFormatException;
import org.ini4j.Wini;

import sample.log.Utils;
import util.FileHelper;
import util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

/**
 * Created by Yongtao on 9/20/2015.
 * <p/>
 * Read file from server to memory. Path is read from console
 */
public class FileReadClient{
	private static final Log log=Log.get();

	static Session downloadFile(IOControl control,String ip,int port,String path,long position,long limit) throws Exception{
		Session session=new Session(FileReadMsgType.READ_FILE);
		session.set("path",path);
		if(position>0)
			session.set("position",position);
		if(limit>0)
			session.set("limit",limit);
		return control.request(session,ip,port);
	}

	static String downloadToTemp(Path tempDir,IOControl control,String ip,int port,String path){
		try{
			Session response=downloadFile(control,ip,port,path,0,0);
			if(response.getType()!=FileReadMsgType.READ_FILE_OK) return null;
			File newFile=new File(tempDir.toFile(),response.getString("name"));
			newFile.createNewFile();
			FileOutputStream fos=new FileOutputStream(newFile);
			FileHelper.download(response.getSocketChannel(),fos.getChannel(),response.getLong("size"));
			fos.close();
			return newFile.getAbsolutePath();
		}catch(Exception e){
			log.w(e);
			return null;
		}
	}
	static long readFile(IOControl control,String ip,int port,String path,long position,long limit){
		try{
			Session response=downloadFile(control,ip,port,path,position,limit);
			if(response.getType()!=FileReadMsgType.READ_FILE_OK) return 0;
			long size=response.getLong("size");
			SocketChannel src=response.getSocketChannel();
			ByteBuffer buffer=ByteBuffer.allocateDirect(1024*128);
			long read=0;
			while(read<size){
				long read_once=0;
				while(buffer.hasRemaining() && read<size){
					read_once=src.read(buffer);
					if(read_once<0) break;
					read+=read_once;
					//buffer.mark();
				}
				if(read_once<0) break;
				if(read<size)
					buffer.clear();
					//buffer.reset();
			}
			return read;
		}catch(Exception e){
			log.w(e);
			return 0;
		}
	}
	
//	public static void main(String[] args){
//		IOControl control = new IOControl();
//		Wini conf;
//		try {
//			conf = new Wini(new File("sample.ini"));
//			String serverIP = conf.get("read server", "ip");
//			int serverPort=conf.get("read server","port",int.class);
//			for(int i =0; i < 10000; i++){
//				System.out.println("read :" + readFile(control, serverIP, serverPort, "temp/vk.zip", 0, 0));
//			}
//		} catch (InvalidFileFormatException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
}
