package java8.study;

public class Test {

    public static void main(String[] args) {
        byte l = 112;//byte[0]
        byte l2 = -88;//byte[最後]

        //(l >>> 5) & 1 只要是1 就是邊, 是邊的情況下先<<1 在加上後面的(l2 & 1)來確定是什麼邊（進還是出）
        int r = (((l >>> 5) & 1 ) << 1) + (l2 & 1);
        System.out.println(r);
    }
}
