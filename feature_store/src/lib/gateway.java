import py4j.GatewayServer;
import com.splicemachine.shiro.SpliceDatabaseRealm;
import com.splicemachine.shiro.SpliceDatabaseJWTRealm;
import com.splicemachine.shiro.filter.SpliceAuthenticatingFilter;
import com.splicemachine.shiro.jwt.JWTAuthenticationToken;
public class gateway{
    public static void main(String [] args){
            GatewayServer gatewayServer = new GatewayServer();
            gatewayServer.start();
    }
}
