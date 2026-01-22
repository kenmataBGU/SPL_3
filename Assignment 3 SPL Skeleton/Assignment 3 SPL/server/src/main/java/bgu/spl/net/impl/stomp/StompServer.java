package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;

public class StompServer {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: StompServer <port> <tpc|reactor>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String mode = args[1];

        if (mode.equals("tpc")) {
            Server.threadPerClient(
                    port,
                    StompMessagingProtocolImpl::new, 
                    StompMessageEncoderDecoder::new 
            ).serve();
        } else if (mode.equals("reactor")) {
            Server.reactor(
                    Runtime.getRuntime().availableProcessors(),
                    port,
                    StompMessagingProtocolImpl::new,
                    StompMessageEncoderDecoder::new 
            ).serve();
        } else {
            System.out.println("Error: Unknown server mode. Use 'tpc' or 'reactor'.");
        }
    }
}