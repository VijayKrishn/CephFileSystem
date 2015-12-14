package sample;

import net.*;
import org.ini4j.Wini;
import sample.log.Utils;
import util.Log;

import java.io.File;
import java.io.IOException;

public class GracefulShutdownServer{
	static Log log=Log.get();

	public enum Shut implements MsgType{
		SHUT,OK
	}

	static class ShutHandler implements MsgHandler{
		IOControl control;

		ShutHandler(IOControl control){
			this.control=control;
		}

		@Override
		public boolean process(Session session) throws IOException{
			try{
				control.response(new Session(Shut.OK),session);
				session.getSocket().getOutputStream().flush();
				session.getSocket().close();
			}catch(IOException ignored){
			}
			return false;
		}
	}

	public static void main(String args[]){
		try{
			Utils.connectToLogServer(log);
			//  read conf
			Wini conf=new Wini(new File("conf/sample/sample.ini"));
			int port=conf.get("read server","port",int.class);

			try{
				IOControl server=new IOControl();
				//  register echo handlers
				MsgHandler logger=new FileReadEchoServer.Echo(server);
				server.registerMsgHandlerLast(logger,new EchoMsgType[]{EchoMsgType.ECHO,EchoMsgType.EXIT_SERVER});

				// register filters
				MsgFilter stat=new RawLogger();
				server.registerMsgFilterHead(stat);

				MsgHandler shut=new ShutHandler(server);
				server.registerMsgHandlerHead(shut,Shut.values());

				// start server
				server.startServer(port);

				// blocking until asked to quit (see SimpleEchoClient)
				server.waitForServer();
			}catch(IOException e){
				log.w(e);
			}
		}catch(IOException e){
			log.w(e);
		}
	}
}
