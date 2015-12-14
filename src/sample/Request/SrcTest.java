package sample.Request;

import req.Rand.ContentSrc;

import java.io.IOException;
import java.nio.channels.Channels;

public class SrcTest{
	public static void main(String args[]) throws IOException{
		try(ContentSrc src=new ContentSrc("Test me baby.")){
			src.transferTo(Channels.newChannel(System.out),4096);
		}
	}
}