package WebServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Server {
    final private static int PORT = 8080;

    public static void main(String[] args) throws Exception{
        try(ServerSocket serverSocket = new ServerSocket(PORT)){
            while (true){
                try{
                    Socket client = serverSocket.accept();
                    handleClient(client);
                } catch(Exception err){
                    err.printStackTrace();
                }
            }
        }

    }

    private static void handleClient(Socket client) throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
        List<String> requestLines = new ArrayList<>();
        String line;
        do {
            line = br.readLine();
            requestLines.add(line);

        }while(!line.isBlank());
    }

}
