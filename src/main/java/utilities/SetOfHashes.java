package utilities;

import java.util.ArrayList;
import java.util.List;

public class SetOfHashes {
    public List<SimpleHashFunction> hashes;
    public SetOfHashes(int k,int n) {
        this.hashes = new ArrayList<>();
        for (int i = 0;i <k;i++)
            hashes.add(new SimpleHashFunction(n));
    }
}
