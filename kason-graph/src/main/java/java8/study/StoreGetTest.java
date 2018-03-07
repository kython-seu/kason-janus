package java8.study;

import java.util.HashMap;
import java.util.function.Consumer;

public class StoreGetTest {


    public static void main(String[] args) {
        HashMap<String, Student> map = new HashMap<>();

        map.put("lilu", new Student("lilu", 15));

// :: 适用于Function<T,R> 输入T类型， 输出 R类型
        StoreGet tConsumer = map::get; //把HashMap的get方法赋给了接口，变成了lamda表达式

        Student lilu = tConsumer.get("lilu");
        System.out.println(lilu);
    }
}
