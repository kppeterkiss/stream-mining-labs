package utilities;



import java.io.Serializable;

import java.util.Arrays;

public class BitMap implements Serializable {
    private int[] map;
    public BitMap(int n) {
        System.out.println("INIT");
        this.map = new int[n];
    }

    public void set(int pos) throws IndexOutOfBoundsException{
        this.map[pos] = 1;
    }
    public int getLength(){
        return this.map.length;
    }

    public boolean get(int pos)throws IndexOutOfBoundsException{
        return this.map[pos]>0;
    }
    public void add(int pos){
        this.map[pos]++;
    }
    public int getValue(int pos){
        return this.map[pos];
    }
    @Override
    public String toString(){
        return Arrays.toString(this.map);
    }
}
