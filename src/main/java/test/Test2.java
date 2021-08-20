package test;

public class Test2 extends Test3{
    public static void main(String[] args) {
        Test sub = Test.SUB;
        System.out.println(sub.name());

    }

    Test2(){
        super("fa");
    }
}
