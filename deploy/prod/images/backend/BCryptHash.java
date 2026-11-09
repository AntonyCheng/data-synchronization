/**
 * Prints the BCrypt hash of its argument, as the platform's login check expects it
 * (Hutool BCrypt, the same as RuoYi uses for sys_user.password). Run by install.sh with the
 * Hutool jars next to it: java -cp '/opt/datasync/tools/*' /opt/datasync/tools/BCryptHash.java PASSWORD
 */
public class BCryptHash {
    public static void main(String[] args) {
        if (args.length != 1 || args[0].isEmpty()) {
            System.err.println("usage: BCryptHash <password>");
            System.exit(2);
        }
        System.out.println(cn.hutool.crypto.digest.BCrypt.hashpw(args[0]));
    }
}
