package utilities;


import java.io.Serializable;
import java.util.Random;

public class SimpleHashFunction implements Serializable {
    int multiplier;
    int increment;
    int modulo;

    public SimpleHashFunction(int b){
        Random r= new Random();
        this.modulo = b;
        this.increment=r.nextInt(b);
        this.multiplier=r.nextInt(b);
    }

    public int getIntValue(String s){
        int ans = 0;
        char[] ca = s.toCharArray();
        for (char c : ca)
            ans+=c;
        return ans;
    }

    public int getHash(String value){
        return (getIntValue(value)*multiplier+increment)%modulo;
    }
}