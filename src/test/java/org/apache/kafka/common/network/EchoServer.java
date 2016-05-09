package org.apache.kafka.common.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.ssl.SslFactory;

/**
 * 简单的Socket服务器
 *
 * 服务端接收按照大小分割的字节数组，并输出这些信息到发送端。
 *
 * @author wanggang
 *
 */
class EchoServer extends Thread {

	public final int port;
	private final ServerSocket serverSocket;
	private final List<Thread> threads;
	private final List<Socket> sockets;
	private SecurityProtocol protocol = SecurityProtocol.PLAINTEXT;
	private SslFactory sslFactory;
	private final AtomicBoolean renegotiate = new AtomicBoolean();

	public EchoServer(Map<String, ?> configs) throws Exception {
		this.protocol = configs.containsKey("security.protocol") ? SecurityProtocol
				.valueOf((String) configs.get("security.protocol")) : SecurityProtocol.PLAINTEXT;
		if (protocol == SecurityProtocol.SSL) {
			this.sslFactory = new SslFactory(Mode.SERVER);
			this.sslFactory.configure(configs);
			SSLContext sslContext = this.sslFactory.sslContext();
			this.serverSocket = sslContext.getServerSocketFactory().createServerSocket(0);
		} else {
			this.serverSocket = new ServerSocket(0);
		}
		this.port = this.serverSocket.getLocalPort();
		this.threads = Collections.synchronizedList(new ArrayList<Thread>());
		this.sockets = Collections.synchronizedList(new ArrayList<Socket>());
	}

	public void renegotiate() {
		renegotiate.set(true);
	}

	@Override
	public void run() {
		try {
			while (true) {
				final Socket socket = serverSocket.accept();
				sockets.add(socket);
				Thread thread = new Thread() {

					@Override
					public void run() {
						try {
							DataInputStream input = new DataInputStream(socket.getInputStream());
							DataOutputStream output = new DataOutputStream(socket.getOutputStream());
							while (socket.isConnected() && !socket.isClosed()) {
								int size = input.readInt();
								if (renegotiate.get()) {
									renegotiate.set(false);
									((SSLSocket) socket).startHandshake();
								}
								byte[] bytes = new byte[size];
								input.readFully(bytes);
								output.writeInt(size);
								output.write(bytes);
								output.flush();
							}
						} catch (IOException e) {
							// ignore
						} finally {
							try {
								socket.close();
							} catch (IOException e) {
								// ignore
							}
						}
					}

				};
				thread.start();
				threads.add(thread);
			}
		} catch (IOException e) {
			// ignore
		}
	}

	public void closeConnections() throws IOException {
		for (Socket socket : sockets)
			socket.close();
	}

	public void close() throws IOException, InterruptedException {
		this.serverSocket.close();
		closeConnections();
		for (Thread t : threads)
			t.join();
		join();
	}

}
