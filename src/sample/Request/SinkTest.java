package sample.Request;

import req.Rand.ContentSink;

import java.io.IOException;
import java.nio.channels.Channels;

public class SinkTest{
	public static void main(String args[]) throws IOException{
		ContentSink sink=new ContentSink(1024);
		sink.transferFrom(Channels.newChannel(System.in),4096);
	}
}
