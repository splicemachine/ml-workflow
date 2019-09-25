import py4j.GatewayServer;
import com.splicemachine.shiro.SpliceDatabaseRealm;
public class gateway{
    public static void main(String [] args){
            GatewayServer gatewayServer = new GatewayServer();
            gatewayServer.start();
    }
}
