import py4j.GatewayServer;
import com.splicemachine.shiro.SpliceDatabaseRealm;
import com.splicemachine.shiro.SpliceDatabaseJWTRealm;
import org.apache.shiro.authc.BearerToken;
public class gateway{
    public static void main(String [] args){
            GatewayServer gatewayServer = new GatewayServer();
            gatewayServer.start();
    }
}
