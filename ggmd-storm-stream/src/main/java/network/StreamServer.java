package main.java.network;

import main.java.core.StreamRunners;


import java.net.*;
import java.text.SimpleDateFormat;

import java.io.*;

import java.io.IOException;

public class StreamServer {

	private int port = -1;
	private int delay = 0;


	public StreamServer(String animals, int port) {
		this.port = port;
		this.delay = this.getDelay(animals);

	}

	/*
	affectation des latences en fonction du type d'animal
	* */
	private int getDelay(String a) {

		switch (a) {
			case "tortoise":
				return 5000;
			case "rabbit":
				return 100;
			default:
				return 1000;
		}

	}


	public void send(StreamRunners sr) throws Exception {

		try {
			ServerSocket server = new ServerSocket(this.port);

			int counter = 0;
			System.out.println("Server Started ....");
			Socket serverClient = server.accept();  //server accept the client connection request

			BufferedWriter out = new BufferedWriter(
					new OutputStreamWriter(serverClient.getOutputStream()));

			while (true) {

				out.write(sr.getMessage());
				out.newLine();
				out.flush();
				try {
					Thread.sleep(this.delay);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}

			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}