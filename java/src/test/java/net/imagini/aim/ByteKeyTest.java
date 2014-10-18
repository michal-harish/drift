package net.imagini.aim;

import java.util.TreeMap;

import junit.framework.Assert;
import net.imagini.aim.utils.ByteKey;

import org.junit.Test;

public class ByteKeyTest {

    @Test
    public void testClassifierForTheSameKey() {
        TreeMap<ByteKey,String> tree = new TreeMap<ByteKey,String>();
        ByteKey key2 = new ByteKey("12345678".getBytes(),2); 
        ByteKey key1 = new ByteKey("12345678".getBytes(),1);
        tree.put(key2, "two");
        tree.put(key1, "one");
        Assert.assertEquals(2, tree.size());
        Assert.assertEquals("one", tree.pollFirstEntry().getValue());
        Assert.assertEquals("two", tree.pollFirstEntry().getValue());
    }
}
